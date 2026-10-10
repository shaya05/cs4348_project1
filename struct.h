// struct.h - shared definitions for all four processes (CS 4348 Project 1)
#define _GNU_SOURCE
#define _XOPEN_SOURCE 700

#ifndef STRUCT_H
#define STRUCT_H

#include <cerrno>
#include <cstdarg>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <string>
#include <vector>

#include <fcntl.h>
#include <pthread.h>
#include <signal.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

// ---------------------------------------------------------------------------
// Names of the IPC objects
// ---------------------------------------------------------------------------
// Channel 1: Client -> Dispatcher
#define REQUEST_FIFO_PATH "/tmp/cs4348_dispatcher.fifo"   
// Channel 2: Dispatcher -> Client (+ pid)
#define REPLY_FIFO_PREFIX "/tmp/cs4348_reply_"    
// POSIX shared memory (/dev/shm)
#define SHM_NAME "/cs4348_proj1_shm"
// Channel 3: System V message queue
#define MSGQ_KEY ((key_t)0x43483401)
// Channel 5: Worker -> Dispatcher
#define COMPLETION_SIGNAL SIGRTMIN                        

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------
constexpr uint32_t PROTO_VERSION = 1;
constexpr uint32_t SHM_MAGIC = 0x43483401;
constexpr uint32_t NUM_SLOTS = 64;
constexpr uint32_t NUM_JOB_TYPES = 6;
constexpr size_t PARAMS_LEN = 256;
constexpr size_t FIFO_PATH_LEN = 128;
constexpr size_t DESC_LEN = 128;
constexpr size_t RESULT_LEN = 4096;
constexpr size_t REQUEST_SIZE = 512;
constexpr size_t REPLY_MAX_TOTAL = 4096;
constexpr uint32_t EVENT_RING = 256;
constexpr uint32_t EVENT_TEXT = 96;

// ---------------------------------------------------------------------------
// Channel 1: Request (Client -> Dispatcher), exactly 512 bytes
// ---------------------------------------------------------------------------
enum MsgType : uint32_t {
    MSG_SUBMIT  = 1,
    MSG_STATUS = 2,
    MSG_LIST = 3,
    MSG_RESERVED_QUIT = 4,   // QUIT is handled locally by the Client
    MSG_SHUTDOWN = 5,
    MSG_STATS= 6
};

struct Request {
    uint32_t version;
    uint32_t msg_type;
    int32_t client_pid;
    uint32_t job_type; // SUBMIT only
    uint64_t job_id;                   
    uint32_t param_len;
    char params[PARAMS_LEN];
    char reply_fifo[FIFO_PATH_LEN];
    char padding[100];
};
static_assert(sizeof(Request) == REQUEST_SIZE, "Request must be exactly 512 bytes");

// ---------------------------------------------------------------------------
// Channel 2: Reply (Dispatcher -> Client): 32-byte header + payload_len bytes
// ---------------------------------------------------------------------------
enum ReplyType : uint32_t {
    REPLY_ACK = 1,
    REPLY_RESULT = 2,
    REPLY_STATUS = 3,
    REPLY_LIST = 4,
    REPLY_STATS = 5,
    REPLY_ERROR  = 6
};

enum JobState : uint32_t {
    JOB_QUEUED = 0, 
    JOB_RUNNING = 1, 
    JOB_DONE = 2,
    JOB_FAILED = 3, 
    JOB_TIMED_OUT = 4, 
    JOB_UNKNOWN = 5
};

enum ErrCode : uint32_t {
    ERR_NONE = 0, 
    ERR_BUSY = 1, 
    ERR_BAD_TYPE = 2, 
    ERR_BAD_PARAMS = 3,
    ERR_UNKNOWN_JOB = 4, 
    ERR_BAD_VERSION = 5, 
    ERR_INTERNAL = 6
};

constexpr uint32_t REPLY_FLAG_TRUNCATED = 1;

struct ReplyHeader {
    uint32_t version;
    uint32_t reply_type;
    uint64_t job_id; // job this reply refers to (0 if not job specific)
    uint32_t status; // JobState, or ErrCode for REPLY_ERROR
    uint32_t flags;  // bit 0 = payload truncated
    uint32_t payload_len; // bytes of text following this header
    uint32_t reserved;
};

static_assert(sizeof(ReplyHeader) == 32, "ReplyHeader must be 32 bytes");
constexpr size_t REPLY_MAX_PAYLOAD = REPLY_MAX_TOTAL - sizeof(ReplyHeader);

// ---------------------------------------------------------------------------
// Channel 3: Job message (Dispatcher -> Worker), System V message queue
// msgsnd/msgrcv size = sizeof(JobMsg) - sizeof(long)
// ---------------------------------------------------------------------------
struct JobMsg {
    long mtype;        // always 1
    uint64_t job_id;
    uint32_t slot_index;
    uint32_t job_type;
    int32_t  client_pid;
    int64_t  time_received; //tracked using epoch milliseconds
    char params[PARAMS_LEN];
};

// ---------------------------------------------------------------------------
// Channel 4: Shared memory
// ---------------------------------------------------------------------------
enum SlotState : uint32_t { 
    SLOT_FREE = 0, 
    SLOT_ASSIGNED = 1, 
    SLOT_RUNNING = 2, 
    SLOT_DONE = 3 
};

enum Outcome : uint32_t { 
    OUTCOME_OK = 0, 
    OUTCOME_ERROR = 1, 
    OUTCOME_TRUNCATED = 2 
};

struct Slot {
    uint32_t state; // SlotState: decides who may write this slot
    uint8_t delivered; // set by Dispatcher
    uint8_t logged; // set by Logger
    uint8_t pad0[2];
    uint64_t job_id;
    int32_t client_pid;
    int32_t  worker_pid;
    uint32_t job_type;
    uint32_t outcome;
    int64_t time_received;         // uses epoch milliseconds
    int64_t time_completed;        // uses epoch milliseconds
    uint32_t result_len;
    uint32_t pad1;
    char description[DESC_LEN];
    char result[RESULT_LEN];
};

enum EventType : uint32_t {
    EV_DISPATCHER_START = 1, 
    EV_DISPATCHER_STOP, 
    EV_WORKER_START, 
    EV_WORKER_STOP,
    EV_JOB_SUBMITTED, 
    EV_JOB_REJECTED, 
    EV_JOB_STARTED, 
    EV_JOB_FINISHED,
    EV_SIGNAL_SENT, 
    EV_SIGNAL_RECEIVED, 
    EV_JOB_DELIVERED, 
    EV_CLIENT_UNREACHABLE,
    EV_WARNING
};

struct Event {
    uint64_t seq;
    int64_t time_ms;
    int32_t pid;
    uint32_t type;
    uint64_t job_id;
    char text[EVENT_TEXT];
};

struct SharedHeader {
    uint32_t magic; // writtenonce everything is initialised
    uint32_t version;
    int32_t dispatcher_pid; // target of the completion signal
    uint32_t num_slots;
    uint32_t shutdown; // Dispatcher sets to 1 so the Logger can drain and exit
    uint32_t pad;
    pthread_mutex_t stats_lock;  // protects counters below
    uint64_t jobs_completed;
    uint64_t jobs_failed;
    uint64_t per_type_count[NUM_JOB_TYPES];
    uint64_t total_turnaround_ms;
    pthread_mutex_t event_lock; // protects the event ring
    uint64_t event_head; // next sequence number to write
    Event events[EVENT_RING];
};

struct SharedRegion {
    SharedHeader header;
    Slot slots[NUM_SLOTS];
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
inline int64_t now_ms() {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (int64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

inline std::string format_ms(int64_t ms) {
    if (ms <= 0) return "-";
    time_t s = (time_t)(ms / 1000);
    struct tm tmv;
    localtime_r(&s, &tmv);
    char b[48];
    size_t n = strftime(b, sizeof b, "%Y-%m-%d %H:%M:%S", &tmv);
    snprintf(b + n, sizeof b - n, ".%03d", (int)(ms % 1000));
    return b;
}

inline const char* job_name(uint32_t t) {
    static const char* names[] = {"Unknown", "Sort numbers", "Primes in range", "Fibonacci", "Primality test", "Prime factorization", "Palindrome check"};
    return t <= NUM_JOB_TYPES ? names[t] : names[0];
}

inline const char* job_state_name(uint32_t s) {
    static const char* names[] = {"QUEUED", "RUNNING", "DONE", "FAILED", "TIMED_OUT", "UNKNOWN"};
    return s <= JOB_UNKNOWN ? names[s] : names[JOB_UNKNOWN];
}

inline const char* err_name(uint32_t c) {
    static const char* names[] = {"NONE", "BUSY", "BAD_TYPE", "BAD_PARAMS", "UNKNOWN_JOB", "BAD_VERSION", "INTERNAL"};
    return c <= ERR_INTERNAL ? names[c] : "?";
}

inline const char* event_name(uint32_t t) {
    static const char* names[] = {"?", "DISPATCHER_START", "DISPATCHER_STOP", "WORKER_START", "WORKER_STOP", "JOB_SUBMITTED", "JOB_REJECTED", "JOB_STARTED", "JOB_FINISHED", "SIGNAL_SENT", "SIGNAL_RECEIVED", "JOB_DELIVERED", "CLIENT_UNREACHABLE", "WARNING"};
    return t <= EV_WARNING ? names[t] : names[0];
}

// Lock a (robust) process-shared mutex. If a process died while holding it,
// mark it consistent so the system keeps running.
inline void shm_lock(pthread_mutex_t* m) {
    int rc = pthread_mutex_lock(m);
    if (rc == EOWNERDEAD) pthread_mutex_consistent(m);
}
inline void shm_unlock(pthread_mutex_t* m) { pthread_mutex_unlock(m); }

// Append an event to the shared ring buffer (read by the Logger).
inline void post_event(SharedRegion* r, uint32_t type, int32_t pid, uint64_t job_id, const char* fmt, ...) __attribute__((format(printf, 5, 6)));
inline void post_event(SharedRegion* r, uint32_t type, int32_t pid, uint64_t job_id,const char* fmt, ...) {
    char text[EVENT_TEXT] = {0};
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(text, sizeof text, fmt, ap);
    va_end(ap);
    shm_lock(&r->header.event_lock);
    Event& e = r->header.events[r->header.event_head % EVENT_RING];
    e.seq     = r->header.event_head;
    e.time_ms = now_ms();
    e.pid     = pid;
    e.type    = type;
    e.job_id  = job_id;
    memcpy(e.text, text, sizeof e.text);
    r->header.event_head++;
    shm_unlock(&r->header.event_lock);
}

// Attach to the shared memory created by the Dispatcher; waits up to
// wait_seconds for it to appear and be fully initialised.
inline SharedRegion* attach_shared(int wait_seconds) {
    for (int i = 0; i < wait_seconds * 5; i++) {
        int fd = shm_open(SHM_NAME, O_RDWR, 0600);
        if (fd >= 0) {
            struct stat st;
            if (fstat(fd, &st) == 0 && (size_t)st.st_size >= sizeof(SharedRegion)) {
                void* p = mmap(nullptr, sizeof(SharedRegion), PROT_READ | PROT_WRITE,
                               MAP_SHARED, fd, 0);
                close(fd);
                if (p == MAP_FAILED) return nullptr;
                SharedRegion* r = (SharedRegion*)p;
                while (__atomic_load_n(&r->header.magic, __ATOMIC_ACQUIRE) != SHM_MAGIC) {
                    usleep(50000);
                }
                return r;
            }
            close(fd);
        }
        usleep(200000);
    }
    return nullptr;
}

// ---------------------------------------------------------------------------
// Job parameter validation (used by Client for fast feedback and by the
// Dispatcher as the authoritative check)
// ---------------------------------------------------------------------------
inline std::vector<std::string> split_ws(const std::string& s) {
    std::vector<std::string> out;
    size_t i = 0;
    while (i < s.size()) {
        while (i < s.size() && (s[i] == ' ' || s[i] == '\t')) i++;
        size_t j = i;
        while (j < s.size() && s[j] != ' ' && s[j] != '\t') j++;
        if (j > i) out.push_back(s.substr(i, j - i));
        i = j;
    }
    return out;
}

inline bool parse_u64(const std::string& t, uint64_t& out) {
    if (t.empty() || t.size() > 20) return false;
    for (char c : t) if (c < '0' || c > '9') return false;
    errno = 0;
    unsigned long long v = strtoull(t.c_str(), nullptr, 10);
    if (errno == ERANGE) return false;
    out = v;
    return true;
}

inline bool parse_i64(const std::string& t, int64_t& out) {
    if (t.empty() || t.size() > 20) return false;
    size_t i = (t[0] == '-') ? 1 : 0;
    if (i == t.size()) return false;
    for (size_t k = i; k < t.size(); k++) if (t[k] < '0' || t[k] > '9') return false;
    errno = 0;
    long long v = strtoll(t.c_str(), nullptr, 10);
    if (errno == ERANGE) return false;
    out = v;
    return true;
}

inline bool validate_params(uint32_t type, const std::string& p, std::string& err) {
    if (p.size() >= PARAMS_LEN) { err = "parameters too long (max 255 characters)"; return false; }
    std::vector<std::string> tok = split_ws(p);
    uint64_t a = 0, b = 0;
    switch (type) {
    case 1: {
        if (tok.empty() || tok.size() > 100) { err = "give 1 to 100 integers"; return false; }
        int64_t v;
        for (auto& t : tok) if (!parse_i64(t, v)) { err = "'" + t + "' is not an integer"; return false; }
        return true;
    }
    case 2:
        if (tok.size() != 2 || !parse_u64(tok[0], a) || !parse_u64(tok[1], b)) {
            err = "usage: <lo> <hi>"; return false;
        }
        if (a > b)       { err = "lo must be <= hi"; return false; }
        if (b > 1000000) { err = "hi must be <= 1,000,000"; return false; }
        return true;
    case 3:
        if (tok.size() != 1 || !parse_u64(tok[0], a)) { err = "usage: <n>"; return false; }
        if (a < 1 || a > 90) { err = "n must be between 1 and 90"; return false; }
        return true;
    case 4:
        if (tok.size() != 1 || !parse_u64(tok[0], a)) { err = "usage: <n> (unsigned 64-bit)"; return false; }
        return true;
    case 5:
        if (tok.size() != 1 || !parse_u64(tok[0], a)) { err = "usage: <n> (unsigned 64-bit)"; return false; }
        if (a < 2) { err = "n must be >= 2"; return false; }
        return true;
    case 6: {
        size_t i = p.find_first_not_of(" \t");
        if (i == std::string::npos) { err = "usage: <flags> <text>"; return false; }
        size_t j = p.find_first_of(" \t", i);
        if (j == std::string::npos) { err = "missing text to check"; return false; }
        std::string flags = p.substr(i, j - i);
        size_t k = p.find_first_not_of(" \t", j);
        if (k == std::string::npos) { err = "missing text to check"; return false; }
        std::string input = p.substr(k);
        if (flags != "-") {
            size_t s = 0;
            while (s <= flags.size()) {
                size_t c = flags.find(',', s);
                std::string f = flags.substr(s, c == std::string::npos ? std::string::npos : c - s);
                if (f != "ic" && f != "ia") { err = "flags must be '-' or a comma list of ic,ia"; return false; }
                if (c == std::string::npos) break;
                s = c + 1;
            }
        }
        if (input.size() > 200) { err = "text too long (max 200 characters)"; return false; }
        for (char c : input) if (c < 32 || c > 126) { err = "text must be printable ASCII"; return false; }
        return true;
    }
    default:
        err = "unknown job type";
        return false;
    }
}

inline std::string make_description(uint32_t type, const std::string& params) {
    std::string d = job_name(type);
    d += " (" + params.substr(0, 60) + (params.size() > 60 ? "..." : "") + ")";
    return d;
}

#endif // STRUCT_H