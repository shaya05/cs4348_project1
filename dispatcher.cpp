// Name: Aditya Singh, Akshaya Muthusamy
// dispatcher.cpp - central coordinator.
#include "struct.h"

#include <map>
#include <poll.h>
#include <sys/msg.h>

struct JobInfo {
    uint64_t id;
    int32_t client_pid;
    uint32_t type;
    uint32_t state;
    uint32_t slot;
    int32_t worker_pid;
    int64_t t_received;
    int64_t t_done;
    std::string desc;
    std::string reply_fifo;
};

static SharedRegion* g_shm = nullptr;
static int g_qid = -1;
static int g_fifo_fd = -1;
static int g_fifo_keepalive = -1;   // our own write end so poll() never sees EOF
static int g_sigpipe[2] = {-1, -1}; // self-pipe: signal handler -> main loop
static volatile sig_atomic_t g_stop = 0;
static uint64_t g_next_id = 1;
static uint64_t g_submitted = 0, g_rejected = 0;
static int64_t g_start_ms = 0;
static std::map<uint64_t, JobInfo> g_jobs;                // job table (outlives the shm slot)

// ---------------------------------------------------------------- signals
// Completion signal: only async-signal-safe work here. The slot index that
// the Worker passed via sigqueue() is pushed into the self-pipe; the main
// loop does the real work.
static void on_completion(int, siginfo_t* si, void*) {
    int saved = errno;
    int32_t slot = si->si_value.sival_int;
    ssize_t r = write(g_sigpipe[1], &slot, sizeof slot);
    (void)r;
    errno = saved;
}
static void on_term(int) { g_stop = 1; }

// ---------------------------------------------------------------- replies
static bool send_reply(const std::string& fifo, uint32_t type, uint64_t job_id, uint32_t status,
                       uint32_t flags, const std::string& payload) {
    // Only ever write to a reply FIFO that really is a FIFO we expect.
    if (fifo.compare(0, strlen(REPLY_FIFO_PREFIX), REPLY_FIFO_PREFIX) != 0) return false;
    struct stat st;
    if (lstat(fifo.c_str(), &st) != 0 || !S_ISFIFO(st.st_mode)) return false;

    int fd = open(fifo.c_str(), O_WRONLY | O_NONBLOCK);
    if (fd < 0) return false;   // client gone (ENXIO)

    size_t plen = payload.size();
    if (plen > REPLY_MAX_PAYLOAD) { plen = REPLY_MAX_PAYLOAD; flags |= REPLY_FLAG_TRUNCATED; }

    std::vector<char> buf(sizeof(ReplyHeader) + plen);
    ReplyHeader h;
    memset(&h, 0, sizeof h);
    h.version = PROTO_VERSION;
    h.reply_type = type;
    h.job_id = job_id;
    h.status = status;
    h.flags = flags;
    h.payload_len = (uint32_t)plen;
    memcpy(buf.data(), &h, sizeof h);
    memcpy(buf.data() + sizeof h, payload.data(), plen);

    // One write <= PIPE_BUF is atomic: header and payload can't be split.
    ssize_t n = write(fd, buf.data(), buf.size());
    close(fd);
    return n == (ssize_t)buf.size();
}

static void send_error(const std::string& fifo, uint64_t job_id, uint32_t code, const std::string& msg) {
    send_reply(fifo, REPLY_ERROR, job_id, code, 0, msg);
}

// ---------------------------------------------------------------- slots
static int find_free_slot() {
    for (uint32_t i = 0; i < NUM_SLOTS; i++)
        if (__atomic_load_n(&g_shm->slots[i].state, __ATOMIC_ACQUIRE) == SLOT_FREE) return (int)i;
    return -1;
}

// A Worker finished slot `idx`: read the result out of shared memory and
// reply to the Client. Safe to call repeatedly (guarded by `delivered`).
static void deliver_slot(uint32_t idx) {
    if (idx >= NUM_SLOTS) return;
    Slot& s = g_shm->slots[idx];
    if (__atomic_load_n(&s.state, __ATOMIC_ACQUIRE) != SLOT_DONE) return;
    if (__atomic_load_n(&s.delivered, __ATOMIC_ACQUIRE)) return;

    uint32_t len = s.result_len > RESULT_LEN - 1 ? (uint32_t)RESULT_LEN - 1 : s.result_len;
    std::string result(s.result, len);
    uint64_t id = s.job_id;
    bool ok = false;

    auto it = g_jobs.find(id);
    if (it != g_jobs.end()) {
        JobInfo& j = it->second;
        j.state = (s.outcome == OUTCOME_ERROR) ? JOB_FAILED : JOB_DONE;
        j.t_done = s.time_completed;
        j.worker_pid = s.worker_pid;
        std::string payload = j.desc + "\n" + result;
        ok = send_reply(j.reply_fifo, REPLY_RESULT, id, j.state,
                        s.outcome == OUTCOME_TRUNCATED ? REPLY_FLAG_TRUNCATED : 0, payload);
    }
    if (ok) {
        post_event(g_shm, EV_JOB_DELIVERED, getpid(), id, "result sent to client " + std::to_string(s.client_pid));
        printf("[dispatcher] job %llu done by worker %d -> result delivered to client %d\n",
               (unsigned long long)id, s.worker_pid, s.client_pid);
    } else {
        post_event(g_shm, EV_CLIENT_UNREACHABLE, getpid(), id, "could not reach client " + std::to_string(s.client_pid));
        printf("[dispatcher] job %llu done but client %d unreachable\n",
               (unsigned long long)id, s.client_pid);
    }
    fflush(stdout);
    __atomic_store_n(&s.delivered, 1, __ATOMIC_RELEASE);
}

// Deliver anything we were not signalled for (a lost signal is harmless),
// and recycle slots that both we and the Logger are finished with.
static void sweep_slots() {
    for (uint32_t i = 0; i < NUM_SLOTS; i++) {
        Slot& s = g_shm->slots[i];
        if (__atomic_load_n(&s.state, __ATOMIC_ACQUIRE) != SLOT_DONE) continue;
        deliver_slot(i);
        if (__atomic_load_n(&s.delivered, __ATOMIC_ACQUIRE) &&
            __atomic_load_n(&s.logged, __ATOMIC_ACQUIRE))
            __atomic_store_n(&s.state, SLOT_FREE, __ATOMIC_RELEASE);
    }
}

// ---------------------------------------------------------------- job table
static void refresh_job(JobInfo& j) {
    if (j.state == JOB_DONE || j.state == JOB_FAILED) return;
    Slot& s = g_shm->slots[j.slot];
    if (s.job_id != j.id) return;
    uint32_t st = __atomic_load_n(&s.state, __ATOMIC_ACQUIRE);
    if (st == SLOT_RUNNING) {
        j.state = JOB_RUNNING;
        j.worker_pid = s.worker_pid;
    } else if (st == SLOT_DONE) {
        j.state = (s.outcome == OUTCOME_ERROR) ? JOB_FAILED : JOB_DONE;
        j.t_done = s.time_completed;
        j.worker_pid = s.worker_pid;
    }
}

static void prune_jobs() {
    if (g_jobs.size() <= 5000) return;
    for (auto it = g_jobs.begin(); it != g_jobs.end() && g_jobs.size() > 4000;) {
        if (it->second.state == JOB_DONE || it->second.state == JOB_FAILED) it = g_jobs.erase(it);
        else ++it;
    }
}

// ---------------------------------------------------------------- handlers
static void handle_submit(const Request& rq, const std::string& fifo) {
    std::string params(rq.params);
    std::string err;

    if (rq.job_type < 1 || rq.job_type > NUM_JOB_TYPES) {
        g_rejected++;
        send_error(fifo, 0, ERR_BAD_TYPE, "unknown job type (valid: 1-6)");
        post_event(g_shm, EV_JOB_REJECTED, getpid(), 0, "bad job type " + std::to_string(rq.job_type) + " from client " + std::to_string(rq.client_pid));
        return;
    }
    if (!validate_params(rq.job_type, params, err)) {
        g_rejected++;
        send_error(fifo, 0, ERR_BAD_PARAMS, err);
        post_event(g_shm, EV_JOB_REJECTED, getpid(), 0, "bad params client " + std::to_string(rq.client_pid) + ": " + err);
        printf("[dispatcher] rejected SUBMIT from client %d: %s\n", rq.client_pid, err.c_str());
        fflush(stdout);
        return;
    }

    int idx = find_free_slot();
    if (idx < 0) {
        g_rejected++;
        send_error(fifo, 0, ERR_BUSY, "BUSY: all job slots are in use, try again shortly");
        post_event(g_shm, EV_JOB_REJECTED, getpid(), 0, "no free slot (client " + std::to_string(rq.client_pid) + ")");
        return;
    }

    uint64_t id = g_next_id++;
    std::string desc = make_description(rq.job_type, params);
    int64_t t = now_ms();

    // Fill the slot completely, then publish it with a release store.
    Slot& s = g_shm->slots[idx];
    s.delivered = 0;
    s.logged = 0;
    s.job_id = id;
    s.client_pid = rq.client_pid;
    s.worker_pid = 0;
    s.job_type = rq.job_type;
    s.outcome = OUTCOME_OK;
    s.time_received = t;
    s.time_completed = 0;
    s.result_len = 0;
    s.result[0] = '\0';
    snprintf(s.description, sizeof s.description, "%.127s", desc.c_str());
    __atomic_store_n(&s.state, SLOT_ASSIGNED, __ATOMIC_RELEASE);

    JobMsg m;
    memset(&m, 0, sizeof m);
    m.mtype = 1;
    m.job_id = id;
    m.slot_index = (uint32_t)idx;
    m.job_type = rq.job_type;
    m.client_pid = rq.client_pid;
    m.time_received = t;
    snprintf(m.params, sizeof m.params, "%s", params.c_str());

    if (msgsnd(g_qid, &m, sizeof(JobMsg) - sizeof(long), IPC_NOWAIT) < 0) {
        int send_errno = errno;                                    // save before other calls change it
        __atomic_store_n(&s.state, SLOT_FREE, __ATOMIC_RELEASE);   // roll back
        g_next_id--;
        g_rejected++;
        bool full = (send_errno == EAGAIN);
        send_error(fifo, 0, full ? ERR_BUSY : ERR_INTERNAL,
                   full ? "BUSY: job queue is full, try again shortly" : "internal error queueing job");
        post_event(g_shm, EV_JOB_REJECTED, getpid(), 0, "msgsnd failed: " + std::string(strerror(send_errno)));
        return;
    }

    JobInfo j;
    j.id = id; j.client_pid = rq.client_pid; j.type = rq.job_type; j.state = JOB_QUEUED;
    j.slot = (uint32_t)idx; j.worker_pid = 0; j.t_received = t; j.t_done = 0;
    j.desc = desc; j.reply_fifo = fifo;
    g_jobs[id] = j;
    prune_jobs();
    g_submitted++;

    post_event(g_shm, EV_JOB_SUBMITTED, getpid(), id, "client " + std::to_string(rq.client_pid) + ": " + desc);
    printf("[dispatcher] job %llu accepted from client %d: %s (slot %d)\n",
           (unsigned long long)id, rq.client_pid, desc.c_str(), idx);
    fflush(stdout);

    // Immediate ACK carries the new job ID back to the Client.
    send_reply(fifo, REPLY_ACK, id, JOB_QUEUED, 0, desc);
}

static std::string describe_job(JobInfo& j) {
    refresh_job(j);
    std::string s = "Job " + std::to_string(j.id) + ": " + j.desc + "\n";
    s += "  state:     " + std::string(job_state_name(j.state)) + "\n";
    s += "  received:  " + format_ms(j.t_received) + "\n";
    if (j.t_done) s += "  completed: " + format_ms(j.t_done) + "\n";
    if (j.worker_pid) s += "  worker:    pid " + std::to_string(j.worker_pid) + "\n";
    return s;
}

static void handle_status(const Request& rq, const std::string& fifo) {
    auto it = g_jobs.find(rq.job_id);
    if (it == g_jobs.end()) {
        send_error(fifo, rq.job_id, ERR_UNKNOWN_JOB,
                   "no job with ID " + std::to_string(rq.job_id));
        return;
    }
    send_reply(fifo, REPLY_STATUS, rq.job_id, it->second.state, 0, describe_job(it->second));
}

static void handle_list(const Request& rq, const std::string& fifo) {
    std::string out;
    int n = 0;
    for (auto& kv : g_jobs) {
        JobInfo& j = kv.second;
        if (j.client_pid != rq.client_pid) continue;
        refresh_job(j);
        out += "  #" + pad_right(std::to_string(j.id), 4) + " " + pad_right(job_state_name(j.state), 9) + " " + j.desc + "\n";
        n++;
    }
    if (n == 0) out = "  (no jobs submitted by this client)\n";
    send_reply(fifo, REPLY_LIST, 0, 0, 0, out);
}

static void handle_stats(const std::string& fifo) {
    shm_lock(&g_shm->header.stats_lock);
    uint64_t done = g_shm->header.jobs_completed, failed = g_shm->header.jobs_failed;
    uint64_t total_ms = g_shm->header.total_turnaround_ms;
    uint64_t per[NUM_JOB_TYPES];
    memcpy(per, g_shm->header.per_type_count, sizeof per);
    shm_unlock(&g_shm->header.stats_lock);

    int in_use = 0;
    for (uint32_t i = 0; i < NUM_SLOTS; i++)
        if (__atomic_load_n(&g_shm->slots[i].state, __ATOMIC_ACQUIRE) != SLOT_FREE) in_use++;

    uint64_t finished = done + failed;
    uint64_t avg_ms = finished ? total_ms / finished : 0;

    std::string s;
    s += "  uptime:            " + std::to_string((now_ms() - g_start_ms) / 1000) + " s\n";
    s += "  jobs submitted:    " + std::to_string(g_submitted) + "\n";
    s += "  jobs rejected:     " + std::to_string(g_rejected) + "\n";
    s += "  jobs completed:    " + std::to_string(done) + " (failed: " + std::to_string(failed) + ")\n";
    s += "  avg turnaround:    " + std::to_string(avg_ms) + " ms\n";
    s += "  slots in use:      " + std::to_string(in_use) + " / " + std::to_string(NUM_SLOTS) + "\n";
    s += "  completions/type: ";
    for (uint32_t i = 0; i < NUM_JOB_TYPES; i++)
        s += " T" + std::to_string(i + 1) + "=" + std::to_string(per[i]);
    s += "\n";
    send_reply(fifo, REPLY_STATS, 0, 0, 0, s);
}

static void handle_request(Request& rq) {
    rq.params[PARAMS_LEN - 1] = '\0';
    rq.reply_fifo[FIFO_PATH_LEN - 1] = '\0';
    std::string fifo = rq.reply_fifo;

    if (rq.version != PROTO_VERSION) {
        send_error(fifo, 0, ERR_BAD_VERSION, "protocol version mismatch");
        return;
    }
    switch (rq.msg_type) {
    case MSG_SUBMIT:   handle_submit(rq, fifo); break;
    case MSG_STATUS:   handle_status(rq, fifo); break;
    case MSG_LIST:     handle_list(rq, fifo); break;
    case MSG_STATS:    handle_stats(fifo); break;
    case MSG_SHUTDOWN:
        printf("[dispatcher] SHUTDOWN requested by client %d\n", rq.client_pid);
        fflush(stdout);
        send_reply(fifo, REPLY_ACK, 0, 0, 0, "Dispatcher shutting down");
        g_stop = 1;
        break;
    default:
        send_error(fifo, 0, ERR_INTERNAL, "unknown request type");
    }
}

// ---------------------------------------------------------------- setup / teardown
static bool setup_ipc() {
    // Remove leftovers from a previous crashed run.
    shm_unlink(SHM_NAME);
    int old = msgget(MSGQ_KEY, 0);
    if (old >= 0) msgctl(old, IPC_RMID, nullptr);
    unlink(REQUEST_FIFO_PATH);

    // --- shared memory
    int fd = shm_open(SHM_NAME, O_CREAT | O_EXCL | O_RDWR, 0600);
    if (fd < 0) { perror("shm_open"); return false; }
    if (ftruncate(fd, sizeof(SharedRegion)) < 0) { perror("ftruncate"); return false; }
    void* p = mmap(nullptr, sizeof(SharedRegion), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    close(fd);
    if (p == MAP_FAILED) { perror("mmap"); return false; }
    g_shm = (SharedRegion*)p;          // zero-filled by ftruncate

    g_shm->header.version = PROTO_VERSION;
    g_shm->header.dispatcher_pid = getpid();
    g_shm->header.num_slots = NUM_SLOTS;
    g_shm->header.shutdown = 0;

    pthread_mutexattr_t attr;
    pthread_mutexattr_init(&attr);
    pthread_mutexattr_setpshared(&attr, PTHREAD_PROCESS_SHARED);
    pthread_mutexattr_setrobust(&attr, PTHREAD_MUTEX_ROBUST);   // survive a Worker dying mid-lock
    pthread_mutex_init(&g_shm->header.stats_lock, &attr);
    pthread_mutex_init(&g_shm->header.event_lock, &attr);
    pthread_mutexattr_destroy(&attr);

    // Everyone else waits for this flag, so it is written last.
    __atomic_store_n(&g_shm->header.magic, SHM_MAGIC, __ATOMIC_RELEASE);

    // --- message queue
    g_qid = msgget(MSGQ_KEY, IPC_CREAT | IPC_EXCL | 0660);
    if (g_qid < 0) { perror("msgget"); return false; }

    // --- request FIFO
    if (mkfifo(REQUEST_FIFO_PATH, 0660) < 0) { perror("mkfifo"); return false; }
    g_fifo_fd = open(REQUEST_FIFO_PATH, O_RDONLY | O_NONBLOCK);
    if (g_fifo_fd < 0) { perror("open fifo"); return false; }
    g_fifo_keepalive = open(REQUEST_FIFO_PATH, O_WRONLY);
    if (g_fifo_keepalive < 0) { perror("open fifo (keepalive)"); return false; }

    // --- self-pipe for the completion signal
    if (pipe(g_sigpipe) < 0) { perror("pipe"); return false; }
    for (int i = 0; i < 2; i++) fcntl(g_sigpipe[i], F_SETFL, fcntl(g_sigpipe[i], F_GETFL) | O_NONBLOCK);
    return true;
}

static void teardown() {
    if (g_shm) {
        post_event(g_shm, EV_DISPATCHER_STOP, getpid(), 0, "dispatcher shutting down");
        __atomic_store_n(&g_shm->header.shutdown, 1, __ATOMIC_RELEASE);
        usleep(500000);                    // let the Logger drain events and finish logging
    }
    if (g_fifo_fd >= 0) close(g_fifo_fd);
    if (g_fifo_keepalive >= 0) close(g_fifo_keepalive);
    unlink(REQUEST_FIFO_PATH);
    if (g_qid >= 0) msgctl(g_qid, IPC_RMID, nullptr);   // blocked Workers get EIDRM and exit
    if (g_shm) munmap(g_shm, sizeof(SharedRegion));
    shm_unlink(SHM_NAME);
}

int main() {
    signal(SIGPIPE, SIG_IGN);
    g_start_ms = now_ms();

    // Install handlers before the shared memory becomes visible, so a Worker
    // can never signal us while the default (terminate) action is active.
    struct sigaction sa;
    memset(&sa, 0, sizeof sa);
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = SA_SIGINFO | SA_RESTART;
    sa.sa_sigaction = on_completion;
    sigaction(COMPLETION_SIGNAL, &sa, nullptr);

    struct sigaction st;
    memset(&st, 0, sizeof st);
    sigemptyset(&st.sa_mask);
    st.sa_handler = on_term;               // no SA_RESTART: interrupt poll()
    sigaction(SIGINT, &st, nullptr);
    sigaction(SIGTERM, &st, nullptr);

    if (!setup_ipc()) { teardown(); return 1; }

    post_event(g_shm, EV_DISPATCHER_START, getpid(), 0, "dispatcher up, " + std::to_string(NUM_SLOTS) + " slots");
    printf("[dispatcher] pid %d ready. FIFO=%s, %u slots\n", getpid(), REQUEST_FIFO_PATH, NUM_SLOTS);
    fflush(stdout);

    while (!g_stop) {
        struct pollfd fds[2] = {{g_fifo_fd, POLLIN, 0}, {g_sigpipe[0], POLLIN, 0}};
        int rc = poll(fds, 2, 500);        // 500 ms timeout doubles as the slot sweep interval
        if (rc < 0) {
            if (errno == EINTR) continue;
            perror("poll");
            break;
        }

        if (fds[1].revents & POLLIN) {
            int32_t slot;
            while (read(g_sigpipe[0], &slot, sizeof slot) == (ssize_t)sizeof slot) {
                post_event(g_shm, EV_SIGNAL_RECEIVED, getpid(), 0, "completion signal for slot " + std::to_string(slot));
                deliver_slot((uint32_t)slot);
            }
        }

        if (fds[0].revents & POLLIN) {
            for (;;) {
                Request rq;
                ssize_t n = read(g_fifo_fd, &rq, sizeof rq);
                if (n == (ssize_t)sizeof rq) { handle_request(rq); continue; }
                if (n < 0 && errno == EINTR) continue;
                if (n > 0) fprintf(stderr, "[dispatcher] discarded short request (%zd bytes)\n", n);
                break;                     // EAGAIN (drained) or error
            }
        }

        sweep_slots();
    }

    printf("[dispatcher] shutting down\n");
    sweep_slots();
    teardown();
    printf("[dispatcher] IPC objects removed. Bye.\n");
    return 0;
}
