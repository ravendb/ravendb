#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif


#include <sys/wait.h>
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

#include "rvn.h"
#include "status_codes.h"
#include "internal_posix.h"

char** parse_cmd_line(char* line)
{
    int argc = 0;
    char** tmp = NULL;
    char** argv = NULL;

    while (!*line) {
        while (*line == ' ' || *line == '\t' || *line == '\n')
            *line++ = '\0';

        if (tmp = realloc(argv, (++argc) * sizeof(char*)) == NULL) {
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

    if (tmp = realloc(argv, (++argc) * sizeof(char*)) == NULL) {
        if (argv != NULL)
            free(argv);
        return NULL;
    }

    argv[argc] = 0;
    return argv;
}

EXPORT int32_t
rvn_kill_process(void* pid, int32_t* detailed_error_code) {
    int rc = kill((pid_t)pid, 9);
    if (rc != 0) {
        *detailed_error_code = errno;
        return FAIL_KILL;
    }
    // calling this to observe the statuc code of the process
    waitpid((pid_t)pid, &rc, WNOHANG);
    return SUCCESS;
}

EXPORT int32_t
rvn_wait_for_close_process(void* pid, int32_t timeout_ms, int32_t* exit_code, int32_t* detailed_error_code) {
    timespec ts;
    sigset_t child_mask;
    sigemptyset(&child_mask);
    sigaddset(&child_mask, SIGCHLD);
    ts.tv_sec = MSEC_TO_SEC(timeout_ms);
    ts.tv_nsec = (timeout_ms % 1000) * 1000000;

    while (1) {
        int status;
        pid_t result = waitpid((pid_t)pid, &status, WNOHANG);
        if (result != -1)
            break;

        *detailed_error_code = errno;
        status = sigtimedwait(&child_mask, NULL, &ts);
        if (status == -1) {
            if (errno == EAGAIN) {
                return FAIL_TIMEOUT;
            }
        }
    }

    *detailed_error_code = status;

    if (WIFEXITED(status)) {
        exit_code = WEXITSTATUS(status);
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
rvn_spawn_process(const char* filename, const char* cmdline, void** pid, void* stdin, void* stdout, void* stderr, int32_t* detailed_error_code) {
    int filesdes_stdout[2] = { -1,-1 };
    int filesdes_stderr[2] = { -1,-1 };
    int filesdes_stdin[2] = { -1,-1 };
    pid_t process_id = -1;
    int thread_cancel_state;
    int32_t rc = SUCCESS;
    char** argv;
    char* line = strdup(cmdline);

    if (line == NULL) {
        rc = FAIL_NOMEM;
        goto error;
    }

    argv = parse_cmd_line(line);
    if (argv == NULL) {
        rc = FAIL_NOMEM;
        goto error;
    }

    // None of this code can be canceled without leaking handles, so just don't allow it
    pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, &thread_cancel_state);

    if (pipe(filesdes_stdout) == -1) {
        rc = FAIL_CREATE_PIPE;
        goto error;
    }
    if (pipe(filesdes_stderr) == -1) {
        rc = FAIL_CREATE_PIPE;
        goto error;
    }
    if (pipe(filesdes_stdin) == -1) {
        rc = FAIL_CREATE_PIPE;
        goto error;
    }

    if (fcntl(filesdes_stderr[0], F_SETFD, FD_CLOEXEC) == -1) {
        rc = FAIL_FCNTL;
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
    else if (pid == 0) {
        while ((dup2(filesdes_stdout[1], STDOUT_FILENO) == -1) && (errno == EINTR)) {}
        while ((dup2(filesdes_stderr[1], STDERR_FILENO) == -1) && (errno == EINTR)) {}
        while ((dup2(filesdes_stdin[0], STDIN_FILENO) == -1) && (errno == EINTR)) {}

        close(filesdes_stdout[1]);
        close(filesdes_stderr[1]);
        close(filesdes_stdin[0]);

        execvpe(filename, argv, NULL);

        // can only get there if we failed to execvpe()
        _exit(errno);
    }

    // close the child end of these
    close(filesdes_stdout[1]);
    close(filesdes_stderr[1]);
    close(filesdes_stdin[0]);

    goto success;

error:
    detailed_error_code = errno;

    if(filesdes_stderr[0] != -1)
        close(filesdes_stderr[0]);
    if(filesdes_stderr[1] != -1)
        close(filesdes_stderr[1]);

    if (filesdes_stdout[0] != -1)
        close(filesdes_stdout[0]);
    if (filesdes_stdout[1] != -1)
        close(filesdes_stdout[1]);

success:
    if(line != NULL)
        free(line);
    if (argv != NULL)
        free(argv);

    *stdout = filesdes_stdout[0];
    *stderr = filesdes_stderr[0];
    *stdin = filesdes_stdin[1];
    *pid = process_id;

    // Restore thread cancel state
    pthread_setcancelstate(thread_cancel_state, &thread_cancel_state);

    return rc;
}
