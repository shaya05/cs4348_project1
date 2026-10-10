// logger.cpp - persistent record of system activity.
#include "struct.h"

static volatile sig_atomic_t g_stop = 0;
static void on_term(int) { g_stop = 1; }

static FILE* g_log = nullptr;
static uint64_t g_next_seq = 0;
static uint64_t g_events_logged = 0, g_jobs_logged = 0, g_events_dropped = 0;

// Write one timestamped line to the log file. The caller builds the text.
static void write_line(const std::string& body) {
    fprintf(g_log, "[%s] %s\n", format_ms(now_ms()).c_str(), body.c_str());
    fflush(g_log);
}

static void drain_events(SharedRegion* shm) {
    std::vector<Event> batch;
    uint64_t dropped = 0;

    shm_lock(&shm->header.event_lock);
    uint64_t head = shm->header.event_head;
    if (head - g_next_seq > EVENT_RING) {          // we fell behind: oldest events were overwritten
        dropped = head - EVENT_RING - g_next_seq;
        g_next_seq = head - EVENT_RING;
    }
    for (uint64_t q = g_next_seq; q < head; q++) batch.push_back(shm->header.events[q % EVENT_RING]);
    g_next_seq = head;
    shm_unlock(&shm->header.event_lock);

    if (dropped) {
        g_events_dropped += dropped;
        write_line("WARNING logger fell behind, " + std::to_string(dropped) + " events lost");
    }
    for (const Event& e : batch) {
        char text[EVENT_TEXT + 1];
        memcpy(text, e.text, EVENT_TEXT);
        text[EVENT_TEXT] = '\0';
        fprintf(g_log, "[%s] EVENT %-18s pid=%-7d job=%-5llu %s\n", format_ms(e.time_ms).c_str(),
                event_name(e.type), e.pid, (unsigned long long)e.job_id, text);
        g_events_logged++;
    }
    fflush(g_log);
}

static void log_completed_jobs(SharedRegion* shm) {
    for (uint32_t i = 0; i < NUM_SLOTS; i++) {
        Slot& s = shm->slots[i];
        if (__atomic_load_n(&s.state, __ATOMIC_ACQUIRE) != SLOT_DONE) continue;
        if (__atomic_load_n(&s.logged, __ATOMIC_ACQUIRE)) continue;

        // DONE slots are read-only for everyone until both flags are set,
        // so no lock is needed to read the fields.
        char summary[81];
        uint32_t n = s.result_len > 80 ? 80 : s.result_len;
        while (n > 0 && (s.result[n - 1] == '\n' || s.result[n - 1] == '\r')) n--;
        for (uint32_t k = 0; k < n; k++) summary[k] = (s.result[k] == '\n' || s.result[k] == '\r') ? ' ' : s.result[k];
        summary[n] = '\0';

        static const char* outcomes[] = {"OK", "ERROR", "TRUNCATED"};
        fprintf(g_log,
                "[%s] JOB id=%llu received=%s completed=%s client_pid=%d worker_pid=%d outcome=%s "
                "desc=\"%.127s\" result=\"%s\"\n",
                format_ms(now_ms()).c_str(), (unsigned long long)s.job_id,
                format_ms(s.time_received).c_str(), format_ms(s.time_completed).c_str(),
                s.client_pid, s.worker_pid, s.outcome <= OUTCOME_TRUNCATED ? outcomes[s.outcome] : "?",
                s.description, summary);
        fflush(g_log);
        g_jobs_logged++;
        __atomic_store_n(&s.logged, 1, __ATOMIC_RELEASE);   // Dispatcher may now recycle the slot
    }
}

int main(int argc, char** argv) {
    const char* path = (argc > 1) ? argv[1] : "cs4348.log";
    g_log = fopen(path, "a");
    if (!g_log) { perror("fopen log"); return 1; }

    struct sigaction sa;
    memset(&sa, 0, sizeof sa);
    sigemptyset(&sa.sa_mask);
    sa.sa_handler = on_term;
    sigaction(SIGINT, &sa, nullptr);
    sigaction(SIGTERM, &sa, nullptr);

    printf("[logger %d] waiting for dispatcher\n", getpid());
    fflush(stdout);
    SharedRegion* shm = attach_shared(30);
    if (!shm) { fprintf(stderr, "[logger] could not attach shared memory (is the dispatcher running?)\n"); return 1; }

    write_line("LOGGER_START pid=" + std::to_string(getpid()) + " logfile=" + path);
    printf("[logger %d] logging to %s\n", getpid(), path);
    fflush(stdout);

    while (!g_stop) {
        drain_events(shm);
        log_completed_jobs(shm);
        if (__atomic_load_n(&shm->header.shutdown, __ATOMIC_ACQUIRE)) {
            usleep(100000);                // final pass: pick up the last events
            drain_events(shm);
            log_completed_jobs(shm);
            break;
        }
        usleep(100000);
    }

    shm_lock(&shm->header.stats_lock);
    uint64_t done = shm->header.jobs_completed, failed = shm->header.jobs_failed;
    uint64_t total = shm->header.total_turnaround_ms;
    shm_unlock(&shm->header.stats_lock);
    uint64_t finished = done + failed;
    uint64_t avg_ms = finished ? total / finished : 0;
    write_line("LOGGER_STOP events_logged=" + std::to_string(g_events_logged) + " jobs_logged=" + std::to_string(g_jobs_logged) + " events_lost=" + std::to_string(g_events_dropped) + " | system: completed=" + std::to_string(done) + " failed=" + std::to_string(failed) + " avg_turnaround_ms=" + std::to_string(avg_ms));
    printf("[logger %d] exiting (%llu events, %llu jobs logged)\n", getpid(), (unsigned long long)g_events_logged, (unsigned long long)g_jobs_logged);
    munmap(shm, sizeof(SharedRegion));
    fclose(g_log);
    return 0;
}
