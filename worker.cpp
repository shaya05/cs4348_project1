// Name: Aditya S, Akshaya M
// worker.cpp - pulls jobs from the message queue, runs them, stores the result in shared memory, and signals the Dispatcher.

#include "struct.h"
#include <sys/msg.h>

static volatile sig_atomic_t g_stop = 0;
static void on_term(int) { g_stop = 1; }

// ---------------------------------------------------------------------------
// STUB - replace with real computations in Milestone 3.
// Fills `out` with the result text and returns the Outcome.
// ---------------------------------------------------------------------------
static uint32_t execute_job(uint32_t type, const char* params, std::string& out) {
    out = std::string("[stub result] ") + job_name(type) + " received params: " + params + "\n";
    return OUTCOME_OK;
}

static void process_job(SharedRegion* shm, const JobMsg& m, int delay_ms) {
    if (m.slot_index >= NUM_SLOTS) {
        post_event(shm, EV_WARNING, getpid(), m.job_id, "bad slot index " + std::to_string(m.slot_index));
        return;
    }
    Slot& s = shm->slots[m.slot_index];

    // Sanity check: the slot must be the one the Dispatcher reserved for this job.
    if (__atomic_load_n(&s.state, __ATOMIC_ACQUIRE) != SLOT_ASSIGNED || s.job_id != m.job_id) {
        post_event(shm, EV_WARNING, getpid(), m.job_id, "slot " + std::to_string(m.slot_index) + " does not match job");
        return;
    }

    // ASSIGNED -> RUNNING: from here on only this Worker writes the slot.
    s.worker_pid = getpid();
    __atomic_store_n(&s.state, SLOT_RUNNING, __ATOMIC_RELEASE);
    post_event(shm, EV_JOB_STARTED, getpid(), m.job_id, "slot " + std::to_string(m.slot_index) + ": " + job_name(m.job_type));
    printf("[worker %d] started job %llu (%s)\n", getpid(), (unsigned long long)m.job_id, job_name(m.job_type));
    fflush(stdout);

    if (delay_ms > 0) usleep((useconds_t)delay_ms * 1000);

    std::string result;
    uint32_t outcome = execute_job(m.job_type, m.params, result);
    if (result.size() >= RESULT_LEN) {
        result.resize(RESULT_LEN - 1);
        outcome = OUTCOME_TRUNCATED;
    }

    // Step 1: write every result field.
    memcpy(s.result, result.c_str(), result.size() + 1);
    s.result_len = (uint32_t)result.size();
    s.outcome = outcome;
    s.time_completed = now_ms();

    // Step 2: update cumulative counters (the only part that needs a lock).
    shm_lock(&shm->header.stats_lock);
    if (outcome == OUTCOME_ERROR) shm->header.jobs_failed++;
    else shm->header.jobs_completed++;
    if (m.job_type >= 1 && m.job_type <= NUM_JOB_TYPES) shm->header.per_type_count[m.job_type - 1]++;
    shm->header.total_turnaround_ms += (uint64_t)(s.time_completed - s.time_received);
    shm_unlock(&shm->header.stats_lock);

    post_event(shm, EV_JOB_FINISHED, getpid(), m.job_id, "outcome=" + std::to_string(outcome) + ", " + std::to_string(s.result_len) + " bytes");

    // Step 3: publish with a release store: RUNNING -> DONE.
    __atomic_store_n(&s.state, SLOT_DONE, __ATOMIC_RELEASE);

    // Tell the Dispatcher which slot finished. sigqueue() carries the slot
    // index and real-time signals queue up instead of merging.
    union sigval v;
    v.sival_int = (int)m.slot_index;
    int rc = -1;
    for (int attempt = 0; attempt < 10; attempt++) {
        rc = sigqueue(shm->header.dispatcher_pid, COMPLETION_SIGNAL, v);
        if (rc == 0 || errno != EAGAIN) break;   // EAGAIN = signal queue momentarily full
        usleep(10000);
    }
    if (rc == 0) {
        post_event(shm, EV_SIGNAL_SENT, getpid(), m.job_id, "signal sent for slot " + std::to_string(m.slot_index));
        printf("[worker %d] finished job %llu, signalled dispatcher\n", getpid(), (unsigned long long)m.job_id);
    } else {
        // The Dispatcher's periodic sweep will still find the DONE slot.
        std::string why = strerror(errno);
        post_event(shm, EV_WARNING, getpid(), m.job_id, "sigqueue failed: " + why);
        printf("[worker %d] finished job %llu, sigqueue failed (%s)\n", getpid(),
               (unsigned long long)m.job_id, why.c_str());
    }
    fflush(stdout);
}

int main(int argc, char** argv) {
    int delay_ms = (argc > 1) ? atoi(argv[1]) : 800;

    struct sigaction sa;
    memset(&sa, 0, sizeof sa);
    sigemptyset(&sa.sa_mask);
    sa.sa_handler = on_term;               // no SA_RESTART: interrupt msgrcv()
    sigaction(SIGINT, &sa, nullptr);
    sigaction(SIGTERM, &sa, nullptr);

    printf("[worker %d] waiting for dispatcher\n", getpid());
    fflush(stdout);
    SharedRegion* shm = attach_shared(30);
    if (!shm) { fprintf(stderr, "[worker] could not attach shared memory (is the dispatcher running?)\n"); return 1; }

    int qid = -1;
    for (int i = 0; i < 150 && qid < 0; i++) {
        qid = msgget(MSGQ_KEY, 0);
        if (qid < 0) usleep(200000);
    }
    if (qid < 0) { perror("msgget"); return 1; }

    post_event(shm, EV_WORKER_START, getpid(), 0, "worker up (delay " + std::to_string(delay_ms) + " ms)");
    printf("[worker %d] ready\n", getpid());
    fflush(stdout);

    while (!g_stop) {
        JobMsg m;
        ssize_t n = msgrcv(qid, &m, sizeof(JobMsg) - sizeof(long), 1, 0);   // blocks
        if (n < 0) {
            if (errno == EINTR) continue;                  // re-check g_stop
            if (errno == EIDRM || errno == EINVAL) {       // queue removed: dispatcher shut down
                printf("[worker %d] message queue removed, exiting\n", getpid());
                break;
            }
            perror("msgrcv");
            break;
        }
        process_job(shm, m, delay_ms);
    }

    if (!__atomic_load_n(&shm->header.shutdown, __ATOMIC_ACQUIRE))
        post_event(shm, EV_WORKER_STOP, getpid(), 0, "worker stopping");
    munmap(shm, sizeof(SharedRegion));
    return 0;
}
