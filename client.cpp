// Name: Aditya Singh, Akshaya Muthusamy

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

// print every complete reply waiting in the reply fifo
// waits up to timeout_ms for data to show up (0 = just check what is already there)
void print_replies(int r_fd, int timeout_ms) {
    // bytes that were read but not parsed yet (a reply may arrive in pieces,
    // and several replies may arrive in one read)
    static std::string pending;

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

    // pull out each full reply: 32 byte header followed by payload_len bytes
    while (pending.size() >= sizeof(ReplyHeader)) {
        ReplyHeader h;
        memcpy(&h, pending.data(), sizeof(h));

        // sanity check so a corrupt message can't make us wait forever
        if (h.version != PROTO_VERSION || h.payload_len > REPLY_MAX_PAYLOAD) {
            printf("received a corrupt reply, discarding it.\n");
            pending.clear();
            return;
        }

        size_t total = sizeof(h) + h.payload_len;
        if (pending.size() < total) {
            break; // rest of this reply has not arrived yet
        }

        std::string payload = pending.substr(sizeof(h), h.payload_len);
        pending.erase(0, total);
        print_reply(h, payload);
    }
}

// main
int main() {

    // begin by starting client process and printing
    printf("starting client process (pid: %d)\n", getpid());
    fflush(stdout);

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

    // connect to shared memory
    SharedRegion* shared_region = attach_shared(30);
        // make sure it works
    if (!shared_region) {
        errormsg("ERROR: unable to attach to shared memory");
        fflush(stdout);
        close(r_fd);
        unlink(reply_path);
    }
    // post client start
    post_event(shared_region, EV_CLIENT_START, getpid(), 0, "client up");

    // writing to a fifo nobody reads should give an error, not kill us
    signal(SIGPIPE, SIG_IGN);

    char buffer[256];
    bool running = true;

    // main loop to get requests and keep checkign for requests
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
        printf("%d: SUBMIT\n", MSG_SUBMIT);
        printf("%d: STATUS\n", MSG_STATUS);
        printf("%d: LIST\n", MSG_LIST);
        printf("%d: QUIT\n", MSG_RESERVED_QUIT);
        printf("%d: SHUTDOWN\n", MSG_SHUTDOWN);
        printf("%d: STATS\n", MSG_STATS);

        // get request from user
        printf("Enter request number (1-6): ");
        fflush(stdout);

        // if process ended, exit loop
        if (fgets(buffer, sizeof(buffer), stdin) == NULL) {
            break;
        }

        // if empty input, repeat loop
        if (buffer[0] == '\n') {
            continue;
        }

        // make sure valid input => an int
        int msg_input = 0;
        if (strlen(buffer) != 2 || buffer[0] < '1' || buffer[0] > '6' || buffer[1] != '\n') {
            printf("invalid input. please try again with a number between 1 and 6.\n");
            fflush(stdout);
            continue;
        }
        // save
        msg_input = buffer[0] - '0';
        request.msg_type = msg_input;

        // handle based on message type
        if (request.msg_type == MSG_RESERVED_QUIT) {
            printf("quitting client process (pid: %d).\n", getpid());
            running = false;
            break;
        } 
        else if (request.msg_type == MSG_SUBMIT) {
            // show job type options
            printf("Job Types:\n");
            for (uint32_t i = 1; i <= NUM_JOB_TYPES; ++i) {
                printf("%u: %s\n", i, job_name(i));
            }

            // ask user for job type
            printf("enter job type (1-6): ");
            fflush(stdout);

            // make sure a job type is entered
            if (fgets(buffer, sizeof(buffer), stdin) != NULL) {
                // make sure job type is int
                if (strlen(buffer) != 2 || buffer[0] < '1' || buffer[0] > '6' || buffer[1] != '\n') {
                    printf("invalid job type. please try again.\n");
                    fflush(stdout);
                    continue;
                }
                // save job type entered
                int job_type_input = buffer[0] - '0';
                request.job_type = job_type_input;
            } else {
                printf("no job type entered, please try again.\n");
                fflush(stdout);
                continue;
            }

            // ask user for job parameters
            printf("enter job parameters: ");
            fflush(stdout);

            // make sure parameters are entered
            if (fgets(buffer, sizeof(buffer), stdin) != NULL) {
                buffer[strcspn(buffer, "\n")] = 0; // remove newline character
                // validate parameter
                std::string err;
                if (!validate_params(request.job_type, std::string(buffer), err)) {
                    printf("invalid job parameters: %s\n", err.c_str());
                    fflush(stdout);
                    continue;
                }
                // save parameters entered
                strncpy(request.params, buffer, sizeof(request.params) - 1);
                request.param_len = strlen(request.params);
            } else {
                printf("no job parameters entered, please try again.\n");
                fflush(stdout);
                continue;
            }
        }
        else if (request.msg_type == MSG_STATUS) {
            // ask user for job id to check on 
            printf("enter job id to check status: ");
            fflush(stdout);

            // make sure a job id is entered
            if (fgets(buffer, sizeof(buffer), stdin) != NULL) {
                unsigned long long job_id = 0;
                // make sure job id is valid and non-reserved
                if (sscanf(buffer, "%llu", &job_id) != 1 || job_id == 0) {
                    printf("invalid job id. please try again.\n");
                    fflush(stdout);
                    continue;
                }
                // save job id entered
                request.job_id = job_id;
            } else {
                printf("no job id entered, please try again.\n");
                fflush(stdout);
                continue;
            }

        }
        else if (request.msg_type == MSG_LIST) {
            printf("requesting list of jobs from dispatcher.\n");
            fflush(stdout);
        }
        else if (request.msg_type == MSG_STATS) {
            printf("requesting stats from dispatcher.\n");
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
        // check if a shutdown so client also exits
        if (request.msg_type == MSG_SHUTDOWN) {
            running = false;
        }   

        fflush(stdout);

        // wait (up to 3 seconds) for the dispatcher's immediate reply:
        // an ACK with the new job id, a status/list/stats answer, or an error
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
