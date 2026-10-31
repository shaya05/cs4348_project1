// Name: Aditya Singh, Akshaya Muthusamy
// Interacts with user

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <poll.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "struct.h"

// note: styled after namedpipe example files in ipc.zip!

// error msg handler - print n exit
void errormsg( const char *msg ) {
    perror( msg );
    exit( 1 );
}

// print one reply from the dispatcher in a readable way
void print_reply(const ReplyHeader &h, const std::string &payload) {
    switch (h.reply_type) {
    case REPLY_ACK:
        // an ACK for a SUBMIT carries the job id the dispatcher assigned
        if (h.job_id != 0) {
            printf("\n[ACK] Job accepted. Your job ID is %llu\n%s\n", (unsigned long long)h.job_id, payload.c_str());
            printf("Use STATUS with job id %llu to check on it.\n", (unsigned long long)h.job_id);
        } else {
            printf("\n[ACK] %s\n", payload.c_str());
        }
        break;
    case REPLY_RESULT:
        printf("\n[RESULT] Job %llu %s\n%s\n", (unsigned long long)h.job_id,
               h.status == JOB_FAILED ? "FAILED" : "finished", payload.c_str());
        if (h.flags & REPLY_FLAG_TRUNCATED) {
            printf("(result was truncated)\n");
        }
        break;
    case REPLY_STATUS:
        printf("\n[STATUS]\n%s", payload.c_str());
        if (h.flags & REPLY_FLAG_TRUNCATED) {
            printf("\n(output was truncated to fit the reply size limit)\n");
        }
        break;
    case REPLY_LIST:
        printf("\n[LIST]\n%s", payload.c_str());
        break;
    case REPLY_STATS:
        printf("\n[STATS]\n%s", payload.c_str());
        break;
    case REPLY_ERROR:
        printf("\n[ERROR %s] %s\n", err_name(h.status), payload.c_str());
        break;
    default:
        printf("\n[?] unknown reply type %u\n", h.reply_type);
    }
    fflush(stdout);
}

// pull everything currently waiting in the reply fifo into `pending`, then print
// each complete reply (32 byte header + payload_len bytes).
// waits up to timeout_ms for data to show up (0 = just check what is already there)
// returns how many replies were printed
int print_replies(int r_fd, int timeout_ms) {
    // bytes that were read but not parsed yet (a reply may arrive in pieces,
    // and several replies may arrive in one read)
    static std::string pending;
    int printed = 0;

    struct pollfd p;
    p.fd = r_fd;
    p.events = POLLIN;
    p.revents = 0;

    if (poll(&p, 1, timeout_ms) > 0) {
        char tmp[REPLY_MAX_TOTAL];
        ssize_t n;
        // fifo is non-blocking so this stops when it is empty
        while ((n = read(r_fd, tmp, sizeof(tmp))) > 0) {
            pending.append(tmp, (size_t)n);
        }
    }

    while (pending.size() >= sizeof(ReplyHeader)) {
        ReplyHeader h;
        memcpy(&h, pending.data(), sizeof(h));

        // sanity check so a corrupt message can't make us wait forever
        if (h.version != PROTO_VERSION || h.payload_len > REPLY_MAX_PAYLOAD) {
            printf("received a corrupt reply, discarding it.\n");
            pending.clear();
            return printed;
        }

        size_t total = sizeof(h) + h.payload_len;
        if (pending.size() < total) {
            break; // rest of this reply has not arrived yet
        }

        std::string payload = pending.substr(sizeof(h), h.payload_len);
        pending.erase(0, total);
        print_reply(h, payload);
        printed++;
    }
    return printed;
}

// ---------------------------------------------------------------------------
// line input that keeps showing replies while the user is typing / idle.
// we wait on BOTH stdin and the reply fifo, so a finished job is printed the
// moment it arrives instead of after the user presses enter.
// stdin is read with read() into our own buffer (not fgets) so poll() and
// buffering never disagree about whether input is waiting.
// ---------------------------------------------------------------------------
static int g_reply_fd = -1;
static std::string g_inbuf;
static bool g_eof = false;

// returns false on end-of-input (ctrl-d / closed pipe)
bool read_line(const char *prompt, std::string &line) {
    printf("%s", prompt);
    fflush(stdout);
    for (;;) {
        size_t nl = g_inbuf.find('\n');
        if (nl != std::string::npos) {
            line = g_inbuf.substr(0, nl);
            g_inbuf.erase(0, nl + 1);
            if (!line.empty() && line.back() == '\r') line.pop_back();
            return true;
        }
        if (g_eof) return false;

        struct pollfd fds[2];
        fds[0].fd = STDIN_FILENO; fds[0].events = POLLIN; fds[0].revents = 0;
        fds[1].fd = g_reply_fd;   fds[1].events = POLLIN; fds[1].revents = 0;
        int rc = poll(fds, 2, -1);
        if (rc < 0) {
            if (errno == EINTR) continue;
            return false;
        }
        // a reply arrived while we were waiting: show it, then redraw the prompt
        if (fds[1].revents & POLLIN) {
            if (print_replies(g_reply_fd, 0) > 0) {
                printf("%s", prompt);
                fflush(stdout);
            }
        }
        if (fds[0].revents & (POLLIN | POLLHUP)) {
            char tmp[256];
            ssize_t n = read(STDIN_FILENO, tmp, sizeof(tmp));
            if (n > 0) {
                g_inbuf.append(tmp, (size_t)n);
            } else if (n == 0) {
                g_eof = true;
                if (!g_inbuf.empty()) { line = g_inbuf; g_inbuf.clear(); return true; }
                return false;
            }
        }
    }
}

// typing b / back / cancel at any sub-prompt returns to the main menu
bool is_back(const std::string &s) {
    return s == "b" || s == "B" || s == "back" || s == "BACK" || s == "cancel";
}

// one line of help per job type, shown during SUBMIT
const char *job_usage(uint32_t t) {
    switch (t) {
    case 1: return "<int> <int> ...   1 to 100 integers            e.g. 5 3 -2 9";
    case 2: return "<lo> <hi>         lo <= hi <= 1000000          e.g. 1 100";
    case 3: return "<n>               1 <= n <= 90                 e.g. 10";
    case 4: return "<n>               unsigned 64-bit              e.g. 97";
    case 5: return "<n>               n >= 2, unsigned 64-bit      e.g. 360";
    case 6: return "<flags> <text>    flags: - or ic,ia            e.g. ic,ia A man, a plan, a canal: Panama";
    default: return "";
    }
}

// main
int main() {

    // begin by starting client process and printing
    printf("starting client process (pid: %d)\n", getpid());
    fflush(stdout);

    // writing to a fifo nobody reads should give an error, not kill us
    signal(SIGPIPE, SIG_IGN);

    // create reply named pipe path for the client
    char reply_path[FIFO_PATH_LEN];
    snprintf(reply_path, sizeof(reply_path), "%s%d", REPLY_FIFO_PREFIX, getpid());

    // create the pipe itself
    unlink(reply_path); // remove if exists
    // make it so only owner can read/write
    if (mkfifo(reply_path, S_IRUSR | S_IWUSR) < 0) {
        errormsg("ERROR: unable to create the reply named pipe");
    }

    // open the reply fifo ONCE and keep it open for the whole run
    // O_RDWR so the dispatcher can open the write end any time (even long after
    // we sent the request) and so we never see end-of-file
    // O_NONBLOCK so reading never gets stuck when nothing has arrived
    int r_fd = open(reply_path, O_RDWR | O_NONBLOCK);
    if (r_fd < 0) {
        unlink(reply_path);
        errormsg("ERROR: unable to open reply fifo");
    }
    g_reply_fd = r_fd;

    // connect to shared memory (waits up to 30 s for the dispatcher)
    SharedRegion* shared_region = attach_shared(30);
    if (!shared_region) {
        // clean up BEFORE exiting (errormsg() never returns)
        close(r_fd);
        unlink(reply_path);
        fprintf(stderr, "ERROR: unable to attach to shared memory (is the dispatcher running?)\n");
        return 1;
    }
    // post client start
    post_event(shared_region, EV_CLIENT_START, getpid(), 0, "client up");

    std::string line;
    bool running = true;

    // main loop: get a request, send it, show the immediate reply.
    // read_line() prints finished jobs while we wait for the user, so results
    // show up even when the client is sitting idle at the menu.
    while (running) {
        // show any results that finished since the last time we looked
        print_replies(r_fd, 0);

        // create a request and set up basic stuff
        Request request;
        memset(&request, 0, sizeof(request)); // allocate the 512 bytes and zero them out
        request.version = PROTO_VERSION; // set the protocol version
        request.client_pid = getpid();
        request.job_id = 0; // default job id (the dispatcher assigns ids on SUBMIT)

        // provide message types
        printf("\n Requests:\n");
        printf("%d: SUBMIT   - submit a new job\n", MSG_SUBMIT);
        printf("%d: STATUS   - check on a job by id\n", MSG_STATUS);
        printf("%d: LIST     - list your jobs\n", MSG_LIST);
        printf("%d: QUIT     - exit this client\n", MSG_RESERVED_QUIT);
        printf("%d: SHUTDOWN - shut down the whole system\n", MSG_SHUTDOWN);
        printf("%d: STATS    - system statistics\n", MSG_STATS);

        // get request from user (end of input => leave the loop)
        if (!read_line("Enter request number (1-6): ", line)) break;

        // if empty input, repeat loop
        if (line.empty()) continue;

        // make sure valid input => a single digit 1-6
        if (line.size() != 1 || line[0] < '1' || line[0] > '6') {
            printf("invalid input. please try again with a number between 1 and 6.\n");
            fflush(stdout);
            continue;
        }
        request.msg_type = line[0] - '0';

        // handle based on message type
        if (request.msg_type == MSG_RESERVED_QUIT) {
            printf("quitting client process (pid: %d).\n", getpid());
            running = false;
            break;
        }
        else if (request.msg_type == MSG_SUBMIT) {
            // show job type options with usage
            printf("Job Types (type b at any prompt to go back):\n");
            for (uint32_t i = 1; i <= NUM_JOB_TYPES; ++i) {
                printf("  %u: %-20s %s\n", i, job_name(i), job_usage(i));
            }

            // ask user for job type
            if (!read_line("enter job type (1-6): ", line)) break;
            if (is_back(line)) { printf("cancelled.\n"); continue; }
            if (line.size() != 1 || line[0] < '1' || line[0] > '6') {
                printf("invalid job type. please try again.\n");
                fflush(stdout);
                continue;
            }
            request.job_type = line[0] - '0';

            // ask user for job parameters, with usage for the chosen type
            printf("usage: %s\n", job_usage(request.job_type));
            if (!read_line("enter job parameters: ", line)) break;
            if (is_back(line)) { printf("cancelled.\n"); continue; }

            // validate parameter (same check the dispatcher does)
            std::string err;
            if (!validate_params(request.job_type, line, err)) {
                printf("invalid job parameters: %s\n", err.c_str());
                fflush(stdout);
                continue;
            }
            strncpy(request.params, line.c_str(), sizeof(request.params) - 1);
            request.param_len = strlen(request.params);
        }
        else if (request.msg_type == MSG_STATUS) {
            // ask user for job id to check on
            if (!read_line("enter job id to check status (b = back): ", line)) break;
            if (is_back(line)) { printf("cancelled.\n"); continue; }

            // make sure job id is a valid, non-reserved number
            uint64_t job_id = 0;
            if (!parse_u64(line, job_id) || job_id == 0) {
                printf("invalid job id. please try again.\n");
                fflush(stdout);
                continue;
            }
            request.job_id = job_id;
        }
        else if (request.msg_type == MSG_LIST) {
            printf("requesting list of jobs from dispatcher.\n");
            fflush(stdout);
        }
        else if (request.msg_type == MSG_STATS) {
            printf("requesting stats from dispatcher.\n");
            fflush(stdout);
        }
        else if (request.msg_type == MSG_SHUTDOWN) {
            printf("asking the dispatcher to shut the system down.\n");
            fflush(stdout);
        }

        // save the reply fifo path in the request message
        strncpy(request.reply_fifo, reply_path, sizeof(request.reply_fifo) - 1);

        // open the dispatcher fifo for writing
        // O_NONBLOCK so we get an error right away if the dispatcher is not running
        // (a normal blocking open would hang forever)
        int d_fd = open(REQUEST_FIFO_PATH, O_WRONLY | O_NONBLOCK);
        // if unable to open, tell the user and let them try again
        if (d_fd < 0) {
            printf("cannot reach the dispatcher (%s). is it running?\n", strerror(errno));
            fflush(stdout);
            continue;
        }

        // write the request message to the dispatcher fifo
        // (512 bytes is less than PIPE_BUF so the write is atomic even with many clients)
        ssize_t bytes_written = write(d_fd, &request, sizeof(Request));
        close(d_fd);
        if (bytes_written != (ssize_t)sizeof(Request)) {
            printf("failed to send the request to the dispatcher.\n");
            fflush(stdout);
            continue;
        }

        // indicate that the request has been sent
        printf("request sent to dispatcher.\n");
        fflush(stdout);
        // check if a shutdown so client also exits
        if (request.msg_type == MSG_SHUTDOWN) {
            running = false;
        }

        // wait (up to 3 seconds) for the dispatcher's immediate reply:
        // an ACK with the new job id, a status/list/stats answer, or an error.
        // this runs before the next menu is drawn, so the new job's info is
        // always shown first.
        print_replies(r_fd, 3000);
    }

    // show anything that arrived while we were quitting
    print_replies(r_fd, 0);

    // clean up: close the reply fifo and remove it
    close(r_fd);
    unlink(reply_path);
    post_event(shared_region, EV_CLIENT_STOP, getpid(), 0, "client exiting");
    munmap(shared_region, sizeof(SharedRegion));
    printf("client process (pid: %d) exiting.\n", getpid());
    fflush(stdout);

    return 0; //the end
}
