#include "struct.h"
#include <iostream>

// clean up function 
void cleanup() {
    // remove the dispatcher fifo
    unlink(REQUEST_FIFO_PATH);
    
    // remove the shared memory object
    shm_unlink(SHM_NAME);
}

// error handling function
void errormsg(const char* msg) {
    perror(msg);
    cleanup();
    exit(1);
}

// main func
int main() {

    // register signal handlers
    signal(SIGINT, [](int) { cleanup(); std::exit(0); });
    signal(SIGTERM, [](int) { cleanup(); std::exit(0); });

    // signal that the dispatcher is starting
    printf("starting dispatcher process (pid: %d)\n", getpid());
    fflush(stdout);

    // create shared memory
    shm_unlink(SHM_NAME); // remove if exists
    int shm_fd = shm_open(SHM_NAME, O_CREAT | O_RDWR, 0600);
    // if couldn't make
    if (shm_fd < 0) {
        errormsg("ERROR: unable to create shared memory object");
    }

    // set the size of the shared memory object
    if (ftruncate(shm_fd, sizeof(SharedRegion)) < 0) {
        errormsg("ERROR: unable to set size of shared memory object");
    }

    // map the shared memory object into the process's address space
    void *shm_ptr = mmap(NULL, sizeof(SharedRegion), PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd, 0);
    close(shm_fd); // close the file descriptor, we don't need it anymore
    // if couldn't map
    if (shm_ptr == MAP_FAILED) {
        errormsg("ERROR: unable to map shared memory object");
    }

    // cast shared mem pointer for known struct and zero out
    SharedRegion *shared_region = (SharedRegion *)shm_ptr;
    std::memset(shared_region, 0, sizeof(SharedRegion));

    // initialize
    shared_region->header.version = PROTO_VERSION;
    shared_region->header.dispatcher_pid = getpid();
    shared_region->header.num_slots = NUM_SLOTS;
    shared_region->header.shutdown = 0;
    shared_region->header.event_head = 0;

    // set up mutexes and do all the modifying
    pthread_mutexattr_t mutex_attr;
    pthread_mutexattr_init(&mutex_attr);
    pthread_mutexattr_setpshared(&mutex_attr, PTHREAD_PROCESS_SHARED);
    pthread_mutexattr_setrobust(&mutex_attr, PTHREAD_MUTEX_ROBUST);

    // make the locks for stats and events
    pthread_mutex_init(&shared_region->header.stats_lock, &mutex_attr);
    pthread_mutex_init(&shared_region->header.event_lock, &mutex_attr);
    pthread_mutexattr_destroy(&mutex_attr);

    // go through slots and set them to empty
    for (uint32_t i = 0; i < NUM_SLOTS; ++i) {
        shared_region->slots[i].state = SLOT_FREE;
    }

    // put in magic number to show everything is set up!
    __atomic_store_n(&shared_region->header.magic, SHM_MAGIC, __ATOMIC_RELEASE);
    printf("dispatcher setup complete (pid: %d)\n", getpid());
    fflush(stdout);

    // create the request FIFO
    unlink(REQUEST_FIFO_PATH); // remove if exists
    if (mkfifo(REQUEST_FIFO_PATH, S_IRUSR | S_IWUSR) < 0) {
        errormsg("ERROR: unable to create request FIFO");
    }

    // open the request fifo
    int r_fd = open(REQUEST_FIFO_PATH, O_RDONLY);
    if (r_fd < 0) {
        errormsg("ERROR: unable to open request FIFO");
    }
    printf("request FIFO opened (fd: %d)\n", r_fd);
    fflush(stdout);

    // request loop
    Request request;
    while (true) {
        // read a request from the FIFO
        ssize_t bytes_read = read(r_fd, &request, sizeof(Request));
        if (bytes_read < 0) {
            errormsg("ERROR: unable to read from request FIFO");
        } else if (bytes_read == 0) {
            printf("request FIFO closed\n");
            fflush(stdout);
            continue;
        }
        if (bytes_read != sizeof(Request)) {
            printf("incomplete read from request FIFO\n");
            fflush(stdout);
        }

        printf("received request: type=%d, job=%d, client_pid=%d\n", request.msg_type, request.job_type, request.client_pid);
        fflush(stdout);

        post_event(shared_region, EV_JOB_SUBMITTED, request.client_pid, request.job_id, "%u type request received", request.msg_type);
    }

    return 0; //the end
}
