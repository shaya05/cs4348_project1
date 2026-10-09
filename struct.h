#ifndef STRUCT_H
#define STRUCT_H

// allow for fixed width integer types
#include <cstdint>

// constants
#define DISPATCHER_FIFO "/tmp/dispatcher_fifo"

// hold message types
enum msg_type {
    SUBMIT = 1,
    STATUS = 2,
    LIST = 3,
    QUIT = 4,
    SHUTDOWN = 5,
    STATS = 6
};

// hold job types
enum job_type {
    JOB_SORT = 1,
    JOB_PRIMES_RANGE = 2,
    JOB_FIB = 3,
    JOB_IS_PRIME = 4,
    JOB_FACTORS = 5,
    JOB_PALINDROME = 6
};

// message format for request from client
struct requestmsg {
    uint32_t version;       //protocol version
    uint32_t msg_type;      //1 = submit, 2 = status, 3 = list, 4 = quit, 5 = shutdown, 6 = stats
    int32_t client_pid;     //sender pid
    uint32_t job_type;      //job type (tracked for SUBMIT)
    uint64_t job_id;        //job id
    uint32_t param_len;     //length of parameter
    char params[256];       //parameter as text
    char reply_fifo[128];   //path of reply fifo from client
    char padding[100];      //zero filled up to 512 bytes
};

#endif  // STRUCT_H