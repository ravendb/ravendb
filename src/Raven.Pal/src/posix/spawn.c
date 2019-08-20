#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <sys/wait.h>
#include <pthread.h>
#include <sys/time.h>
#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/mman.h>
#include <assert.h>
#include <time.h>
#include <string.h>
#include <signal.h>

#include "rvn.h"
#include "status_codes.h"
#include "internal_posix.h"


#define MSEC_TO_SEC(x) (x/1000)
#define MSEC_TO_NANOSEC(x) ((x % 1000) * 1000000)

char** parse_cmd_line(char* line)
{
    int argc = 0;
    char** tmp = NULL;
    char** argv = NULL;

    while (!*line) {
        while (*line == ' ' || *line == '\t' || *line == '\n')
            *line++ = '\0';

        if ((tmp = realloc(argv, (++argc) * sizeof(char*))) == NULL) {
            if(argv != NULL)
                free(argv);
            return NULL;
        }
        argv = tmp;
        argv[argc] = line;
        while (*line != '\0' && *line != ' ' &&
            *line != '\t' && *line != '\n')
            line++;
    }

    if ((tmp = realloc(argv, (++argc) * sizeof(char*))) == NULL) {
        if (argv != NULL)
            free(argv);
        return NULL;
    }

    argv[argc] = 0;
    return argv;
}

EXPORT int32_t
rvn_kill_process(void* pid, int32_t* detailed_error_code) {
    int rc = kill(*(pid_t*)pid, 9);
    if (rc != 0) {
        *detailed_error_code = errno;
        return FAIL_KILL;
    }
    /* calling this to observe the statuc code of the process*/
    waitpid(*(pid_t*)pid, &rc, WNOHANG);
    return SUCCESS;
}

EXPORT int32_t
rvn_wait_for_close_process(void* pid, int32_t timeout_ms, int32_t* exit_code, int32_t* detailed_error_code) {

    sigset_t child_mask;
    sigemptyset(&child_mask);
    sigaddset(&child_mask, SIGCHLD);
    time_t timeout_sec = MSEC_TO_SEC(timeout_ms);
    
    int status;

    time_t start = time(NULL);

    struct timespec houndrad_ms;
    houndrad_ms.tv_sec = 0;
    houndrad_ms.tv_nsec = MSEC_TO_NANOSEC(timeout_ms);

    struct timespec remaining;

    /*busy wait for proc to end, can't use 'sigtimedwait' on OS X*/
    while (1) {
        
        pid_t result = waitpid(*(pid_t*)pid, &status, WNOHANG);
        if (result != -1)
            break;

        if (difftime(time(NULL), start) >= timeout_sec)
        {
            *detailed_error_code = errno;
            return FAIL_TIMEOUT;
        }
        nanosleep((const struct timespec*)&houndrad_ms,&remaining);
    }

    *detailed_error_code = status;

    if (WIFEXITED(status)) {
        *exit_code = WEXITSTATUS(status);
        return SUCCESS;
    }
    else if (WIFSIGNALED(status)) {
        *exit_code = WTERMSIG(status);
    }
    else if (WIFSTOPPED(status)) {
        *exit_code = WSTOPSIG(status);
    }
    return FAIL_WAIT_PID;
}

EXPORT int32_t
rvn_spawn_process(const char* filename, const char* cmdline, void** pid, void** standard_in, void** standard_out, int32_t* detailed_error_code) {
    int filesdes_stdout[2] = { -1,-1 };
    int filesdes_stdin[2] = { -1,-1 };
    pid_t process_id = -1;
    int32_t rc = SUCCESS;
    char** argv = NULL;
    char* line = NULL; 
    int thread_cancel_state;

    /* None of this code can be canceled without leaking handles, so just don't allow it*/
    pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, &thread_cancel_state);

    line = strdup(cmdline);

    if (line == NULL) {
        rc = FAIL_NOMEM;
        goto error;
    }

    argv = parse_cmd_line(line);
    if (argv == NULL) {
        rc = FAIL_NOMEM;
        goto error;
    }

 

    if (pipe(filesdes_stdout) == -1) {
        rc = FAIL_CREATE_PIPE;
        goto error;
    }

    if (pipe(filesdes_stdin) == -1) {
        rc = FAIL_CREATE_PIPE;
        goto error;
    }

    if (fcntl(filesdes_stdout[0], F_SETFD, FD_CLOEXEC) == -1) {
        rc = FAIL_FCNTL;
        goto error;
    }
    if (fcntl(filesdes_stdin[1], F_SETFD, FD_CLOEXEC) == -1) {
        rc = FAIL_FCNTL;
        goto error;
    }

    process_id = vfork();
    if (process_id == -1) {
        rc = FAIL_VFORK;
        goto error;
    }
    else if (process_id == 0) {
        while ((dup2(filesdes_stdout[1], STDOUT_FILENO) == -1) && (errno == EINTR)) {}
        while ((dup2(filesdes_stdout[1], STDERR_FILENO) == -1) && (errno == EINTR)) {}
        while ((dup2(filesdes_stdin[0], STDIN_FILENO) == -1) && (errno == EINTR)) {}

        close(filesdes_stdout[1]);
        close(filesdes_stdin[0]);

        execve(filename, argv, NULL);

        /* can only get there if we failed to execvpe()*/
        _exit(errno);
    }

    /* close the child end of these*/
    close(filesdes_stdout[1]);
    close(filesdes_stdin[0]);

    goto success;

error:
    *detailed_error_code = errno;

    if (filesdes_stdout[0] != -1)
        close(filesdes_stdout[0]);
    if (filesdes_stdout[1] != -1)
        close(filesdes_stdout[1]);

success:
    if(line != NULL)
        free(line);
    if (argv != NULL)
        free(argv);
    
    *standard_out = (void*)(intptr_t)filesdes_stdout[0];
    *standard_in = (void*)(intptr_t)filesdes_stdin[1];
    *pid    = (void*)(intptr_t)process_id;

    /* Restore thread cancel state*/
    pthread_setcancelstate(thread_cancel_state, &thread_cancel_state);

    return rc;
}
