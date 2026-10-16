// Name: Aditya S, Akshaya M
// worker.cpp - pulls jobs from the message queue, runs them, stores the result in shared memory, and signals the Dispatcher.

#include "struct.h"
#include <sys/msg.h>
#include <algorithm>
#include <cctype>

static volatile sig_atomic_t g_stop = 0;
static void on_term(int) { g_stop = 1; }

// execute jobs pased on parameters and job type
static uint32_t execute_job(uint32_t type, const char* params, std::string& out) {
    try
    {
        // nullptr to actual string 
        std::string param_str(params);
        // split into tokens to use for some cases
        auto tokens = split_ws(param_str);
        // use switch case to handle based on job number!
        // dont need to validate since already done with validate_params function
        switch(type) {
            // sort list of numbers
            case 1: {
                // make into nums
                std::vector<int64_t> nums;
                for (const auto& t : tokens) {
                    int64_t v;
                    parse_i64(t, v);
                    nums.push_back(v);
                }
                // use std sort hehe
                std::sort(nums.begin(), nums.end());
                // make into string
                out = "Sorted numbers:\n";
                for (size_t i = 0; i < nums.size(); ++i) {
                    out += std::to_string(nums[i]);
                    if (i + 1 < nums.size()){
                        out += " ";
                    }
                }
                break;
            }
            // find primes in range lo to hi
            case 2: {
                // save lo and hi
                uint64_t lo = 0, hi = 0;
                parse_u64(tokens[0], lo);
                parse_u64(tokens[1], hi);
                std::vector<uint64_t> primes;
                // go through range
                for (uint64_t n = lo; n <= hi; ++n) {
                    if (n < 2) continue;
                    bool is_prime = true;
                    // check if divisible by any number from 2 to sqrt(n)
                    for (uint64_t d = 2; d * d <= n; ++d) {
                        if (n % d == 0) {
                            is_prime = false;
                            break;
                        }
                    }
                    if (is_prime) primes.push_back(n);
                }
                // make into string
                out = "Primes in range [" + std::to_string(lo) + ", " + std::to_string(hi) + "]:\n";
                for (size_t i = 0; i < primes.size(); ++i) {
                    out += std::to_string(primes[i]);
                    if (i + 1 < primes.size()){
                        out += " ";
                    }
                }
                break;
            }
            // fibonacci sequence
            case 3: {
                // save n
                uint64_t n = 0;
                parse_u64(tokens[0], n);
                
                // calculate fibonacci
                std::vector<uint64_t> fib(n);
                if (n >= 1) fib[0] = 0;
                if (n >= 2) fib[1] = 1;
                for (uint64_t i = 2; i < n; ++i) {
                    fib[i] = fib[i - 1] + fib[i - 2];
                }
                // make into string
                out = "Fibonacci sequence (first " + std::to_string(n) + " numbers):\n";
                for (size_t i = 0; i < fib.size(); ++i) {
                    out += std::to_string(fib[i]);
                    if (i + 1 < fib.size()){
                        out += " ";
                    }
                }
                break;
            }
            // check if number is prime
            case 4: {
                // save n
                uint64_t n = 0;
                parse_u64(tokens[0], n);
                // if less than 2 then not prime
                if (n < 2) {
                    out = std::to_string(n) + " is not prime.";
                    return OUTCOME_OK;
                }

                // check if divisible by any number from 2 to sqrt(n)
                bool is_prime = true;
                for (uint64_t d = 2; d * d <= n; ++d) {
                    if (n % d == 0) {
                        is_prime = false;
                        break;
                    }
                }

                // make into string
                if (is_prime) {
                    out = std::to_string(n) + " is prime.";
                } else {
                    out = std::to_string(n) + " is not prime.";
                }
                break;
            }
            // find prime factors of a number
            case 5: {
                // save n
                uint64_t n = 0;
                parse_u64(tokens[0], n);

                uint64_t copy = n;
                std::vector<uint64_t> factors;
                // find prime factors
                for (uint64_t d = 2; d * d <= copy; ++d) {
                    while (copy % d == 0) {
                        factors.push_back(d);
                        copy /= d;
                    }
                }
                if (copy > 1) {
                    factors.push_back(copy);
                }
                // make into string
                out = "Prime factors of " + std::to_string(n) + ":\n";
                for (size_t i = 0; i < factors.size(); ++i) {
                    out += std::to_string(factors[i]);
                    if (i + 1 < factors.size()){
                        out += " ";
                    }
                }
                break;
            }
            // palindrome check
            case 6: {
                // get tags
                size_t start = param_str.find_first_not_of(" \t");
                size_t end = param_str.find_first_of(" \t", start);
                std::string flags = param_str.substr(start, end - start);

                // get text
                size_t tstart = param_str.find_first_not_of(" \t", end);
                std::string text = param_str.substr(tstart);

                // check flags
                bool ignore_case = (flags.find("ic") != std::string::npos);
                bool ignore_alpha = (flags.find("ia") != std::string::npos);

                // process text based on flags
                std::string processed;
                for (char c : text) {
                    if (ignore_alpha && !std::isalnum(static_cast<unsigned char>(c))) continue;
                    if (ignore_case) c = std::tolower(static_cast<unsigned char>(c));
                    processed += c;
                }

                // reverse it
                std::string reversed = processed;
                std::reverse(reversed.begin(), reversed.end());
                
                // check if palindrome
                out = "Text: \"" + text + "\"";
                if (processed == reversed) {
                    out += " is a palindrome.";
                } else {
                    out += " is not a palindrome.";
                }
                break;
            }
            // not one of the six - thus invalid
            default:
                out = "Error: unknown job type " + std::to_string(type);
                return OUTCOME_ERROR;
        }
        return OUTCOME_OK; //if it made all the way through then its okay
    }
    catch(const std::exception& e) {
        out = "Error: exception occurred: " + std::string(e.what());
        return OUTCOME_ERROR;
    }
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

    if (!__atomic_load_n(&shm->header.shutdown, __ATOMIC_ACQUIRE)) {
        post_event(shm, EV_WORKER_STOP, getpid(), 0, "worker stopping");
    }
    munmap(shm, sizeof(SharedRegion));
    return 0;
}
