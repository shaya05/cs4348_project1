#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
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

    char buffer[256];
    bool running = true;

    // main loop to get requests and keep checkign for requests
    while (running) {
        // create a request and set up basic stuff
        Request request;
        memset(&request, 0, sizeof(request)); // allocate the 512 bytes and zero them out
        request.version = PROTO_VERSION; // set the protocol version
        request.client_pid = getpid();
        request.job_id = 0; // default job id

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
        if (sscanf(buffer, "%d", &msg_input) != 1) {
            printf("Invalid input. Please enter a number between 1 and 6.\n");
            fflush(stdout);
            continue;
        }

        //make sure input in range
        if (msg_input < 1 || msg_input > 6) {
            printf("Unknown request. Please enter a number between 1 and 6.\n");
            fflush(stdout);
            continue;
        }
        // save
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
                int job_type_input = 0;
                // make sure job type is int
                if (sscanf(buffer, "%d", &job_type_input) != 1) {
                    printf("invalid job type. please try again.\n");
                    fflush(stdout);
                    continue;
                }
                // make sure job type is valid
                if (job_type_input < 1 || job_type_input > 6) {
                    printf("unknown job type. please try again.\n");
                    fflush(stdout);
                    continue; 
                }
                // save job type entered
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
                unsigned long job_id = 0;
                // make sure job id is valid and non-reserved
                if (sscanf(buffer, "%llu", &request.job_id) != 1 || request.job_id == 0) {
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
            printf("requesting list of jobs from dispatcher...\n");
            fflush(stdout);
        }
        else if (request.msg_type == MSG_STATS) {
            printf("requesting stats from dispatcher...\n");
            fflush(stdout);
        }

        // save the reply fifo path in the request message
        strncpy(request.reply_fifo, reply_path, sizeof(request.reply_fifo) - 1);

        // open the dispatcher fifo for writing
        int d_fd = open(REQUEST_FIFO_PATH, O_WRONLY);
        // if unable to open 
        if (d_fd < 0) {
            unlink(reply_path); // remove the reply fifo before exiting
            errormsg("ERROR: unable to open dispatcher fifo for writing");
        }

        // write the request message to the dispatcher fifo
        if (write(d_fd, &request, sizeof(Request)) < 0) {
            close(d_fd);
            unlink(reply_path); // remove the reply fifo before exiting
            errormsg("ERROR: unable to write request message to dispatcher fifo");
        }

        // indicate that the request has been sent
        printf("request sent to dispatcher.\n");
        fflush(stdout);

        // close the dispatcher fifo
        close(d_fd);

        // open the reply fifo for reading
        int r_fd = open(reply_path, O_RDONLY);
        // if unable to open
        if (r_fd < 0) {
            unlink(reply_path); // remove the reply fifo before exiting
            errormsg("ERROR: unable to open reply fifo for reading");
        }

        // read the reply message from the reply fifo
        char reply_buffer[REPLY_MAX_TOTAL];
        memset(reply_buffer, 0, sizeof(reply_buffer)); // zero out the buffer

        ssize_t bytes_read = read(r_fd, reply_buffer, sizeof(reply_buffer));
        // if unable to read
        if (bytes_read < 0) {
            close(r_fd);
            unlink(reply_path); // remove the reply fifo before exiting
            errormsg("ERROR: unable to read reply message from reply fifo");
        }

        // parse and print reply 
        if (bytes_read >= sizeof(ReplyHeader)) {
            ReplyHeader* reply = (ReplyHeader*)reply_buffer;
            // if error print that out 
            if (reply->reply_type == REPLY_ERROR) {
                printf("Received error reply: code=%u\n", reply->status);
            } else {
                printf("Received reply: type=%u, job_id=%llu, status=%s, flags=%u, payload_len=%u\n",
                       reply->reply_type, reply->job_id, job_state_name(reply->status), reply->flags, reply->payload_len);
            }
            // if there is a result, print it out
            if (reply->payload_len > 0 && bytes_read >= sizeof(ReplyHeader) + reply->payload_len) {
                printf("Payload: %.*s\n", reply->payload_len, reply_buffer + sizeof(ReplyHeader));
            }
        } else {
            printf("Received incomplete reply message.\n");
        }

        // close the reply fifo after reading
        close(r_fd);
        fflush(stdout);
    }

    // clean up: close the reply fifo and remove it
    unlink(reply_path);
    printf("client process (pid: %d) exiting.\n", getpid());
    fflush(stdout);

    return 0; //the end
}
