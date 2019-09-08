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

PRIVATE char** 
parse_cmd_line(char* line, char *filename)
{
    int argc = 2;
    char** argv = malloc(argc);
    argv[0] = filename;
    argv[--argc] = NULL;    
    if (line == NULL || *line == '\0')
        return argv;
    
    char * pch;
    pch = strtok (line," \t\n");
    while (pch != NULL)
    {        
        argv[argc++] = pch;
        pch = strtok (NULL, " \t\n");
        argv = realloc(argv, argc + 1);
    }  
    argv[argc] = NULL;

    return argv;
}

PRIVATE ssize_t 
ReadSize(int fd, void* buffer, size_t count)
{
    ssize_t rv = 0;
    while (count > 0)
    {
        ssize_t result = read(fd, buffer, count);
        while ( result < 0 && errno == EINTR)
            result = read(fd, buffer, count);

        if (result > 0)
        {
            rv += result;
            buffer = (uint8_t*)buffer + result;
            count -= (size_t)result;
        }
        else
        {
            return -1;
        }
    }
    return rv;
}

PRIVATE void 
CloseIfOpen(int fd)
{
    if (fd >= 0)    
        close(fd);    
}

PRIVATE ssize_t WriteSize(int fd, const void* buffer, size_t count)
{
    ssize_t rv = 0;
    while (count > 0)
    {
        ssize_t result = write(fd, buffer, count);
        while ( result  < 0 && errno == EINTR)
            result = write(fd, buffer, count);

        if (result > 0)
        {
            rv += result;
            buffer = (const uint8_t*)buffer + result;
            count -= (size_t)result;
        }
        else
        {
            return -1;
        }
    }
    return rv;
}

PRIVATE void 
ExitChild(int pipeToParent, int error)
{
    if (pipeToParent != -1)
        WriteSize(pipeToParent, &error, sizeof(error));

    _exit(error != 0 ? error : EXIT_FAILURE);
}

EXPORT int32_t
rvn_spawn_process(const char* filename, char* cmdline, void** pid, void** standard_in, void** standard_out, int32_t* detailed_error_code) {    
    int stdinFds[2] = {-1, -1}, stdoutFds[2] = {-1, -1}, waitForChildToExecPipe[2] = {-1, -1};
    pid_t processId = -1;
    int thread_cancel_state;
    sigset_t signal_set;
    sigset_t old_signal_set;

    char *modified = malloc(strlen(filename) + 3);
    *modified = '\0';
    if (filename[0] != '/')
        strcpy(modified, "./");
    strcat(modified, (char *)filename);

    char **argv = parse_cmd_line(cmdline, modified);

    int32_t rc = SUCCESS;

    pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, &thread_cancel_state);
    
    if ((rc = rvn_pipe_for_child(stdinFds)) != SUCCESS)
        goto error_cleanup;
    if ((rc = rvn_pipe_for_child(stdoutFds)) != SUCCESS)
        goto error_cleanup;
    if ((rc = rvn_pipe_for_child(waitForChildToExecPipe)) != SUCCESS)
        goto error_cleanup;
    
    sigfillset(&signal_set);
    pthread_sigmask(SIG_SETMASK, &signal_set, &old_signal_set);

    if ((processId = vfork()) == 0) /* processId == 0 if this is child process */
    {
        ssize_t result;
        sigset_t junk_signal_set;
        struct sigaction sa_default;
        struct sigaction sa_old;
        memset(&sa_default, 0, sizeof(sa_default)); 
        sa_default.sa_handler = SIG_DFL;

        int sig;
        for (sig = 1; sig < NSIG; ++sig)
        {
            if (sig == SIGKILL || sig == SIGSTOP)
                continue;

            if (!sigaction(sig, NULL, &sa_old))
            {
                void (*oldhandler)(int) = (sa_old.sa_flags & SA_SIGINFO) ? (void (*)(int))sa_old.sa_sigaction : sa_old.sa_handler;
                if (oldhandler != SIG_IGN && oldhandler != SIG_DFL)
                    sigaction(sig, &sa_default, NULL);
            }
        }
        pthread_sigmask(SIG_SETMASK, &old_signal_set, &junk_signal_set); 
        
        /* redirect both stdout and stderr to the same stream, and redirect stdin: */
        result = dup2(stdoutFds[1], STDOUT_FILENO);
        while ( result < 0 && errno == EINTR)
            result = dup2(stdoutFds[1], STDOUT_FILENO);
        if (result == -1)
            ExitChild(waitForChildToExecPipe[1], errno);

        result = dup2(stdoutFds[1], STDERR_FILENO);
        while ( result < 0 && errno == EINTR)
            result = dup2(stdoutFds[1], STDERR_FILENO);
        if (result == -1)
            ExitChild(waitForChildToExecPipe[1], errno);

        result = dup2(stdinFds[0], STDIN_FILENO);
        while ( result < 0 && errno == EINTR)
            result = dup2(stdinFds[0], STDERR_FILENO);
        if (result == -1)
            ExitChild(waitForChildToExecPipe[1], errno);
        
        execvp(modified, argv);
        ExitChild(waitForChildToExecPipe[1], errno);
    }

    pthread_sigmask(SIG_SETMASK, &old_signal_set, &signal_set);

    if (processId < 0)
    {        
        rc = FAIL_VFORK;
        goto error_cleanup;
    }
    *pid = (void*)(intptr_t)processId;
    *standard_in = (void*)(intptr_t)stdinFds[1];
    *standard_out = (void*)(intptr_t)stdoutFds[0];

error_cleanup:
    *detailed_error_code = errno;
    CloseIfOpen(stdinFds[0]);
    CloseIfOpen(stdoutFds[1]);
    CloseIfOpen(waitForChildToExecPipe[1]);
   
    if (waitForChildToExecPipe[0] != -1)
    {
        int childError;
        if (rc == SUCCESS)
        {
            ssize_t result = ReadSize(waitForChildToExecPipe[0], &childError, sizeof(childError));
            if (result == sizeof(childError))
            {
                rc = FAIL_CHILD_PROCESS_FAILURE;
                *detailed_error_code = childError;
            }
        }
        CloseIfOpen(waitForChildToExecPipe[0]);
    }

    if (rc != SUCCESS)
    {
        CloseIfOpen(stdinFds[1]);
        CloseIfOpen(stdoutFds[0]);
        
        if (processId > 0)
        {
            int status;
            waitpid(processId, &status, 0);
        }

        *pid = (void*)(intptr_t)-1;
    }

    free(modified);

    pthread_setcancelstate(thread_cancel_state, &thread_cancel_state);

    return rc;
}

EXPORT int32_t
rvn_kill_process(void* pid, int32_t* detailed_error_code) {    
    int rc = kill((pid_t)(int64_t)(int *)pid, 9);
    if (rc != 0) {
        *detailed_error_code = errno;
        return FAIL_KILL;
    }
    /* calling this to observe the statuc code of the process*/
    waitpid((pid_t)(int64_t)(int *)pid, &rc, WNOHANG);
    return SUCCESS;
}

EXPORT int32_t
rvn_wait_for_close_process(void* pid, int32_t timeout_seconds, int32_t* exit_code, int32_t* detailed_error_code) {
    /* sigset_t child_mask;
    sigemptyset(&child_mask);
    sigaddset(&child_mask, SIGCHLD);
    time_t timeout_sec = MSEC_TO_SEC(timeout_ms);
    
    int status;

    time_t start = time(NULL);

    struct timespec houndrad_ms;
    houndrad_ms.tv_sec = 0;
    houndrad_ms.tv_nsec = MSEC_TO_NANOSEC(250);

    struct timespec remaining;
    busy wait for proc to end, can't use 'sigtimedwait' on OS X
    while (1) {
        pid_t endId = waitpid((pid_t)((int64_t)pid), &status, WNOHANG);
        if (endId == -1)
        {
            *detailed_error_code = errno;
            if(errno == ECHILD)
                return FAIL_CHILD_PROCESS_FAILURE;
            return FAIL_WAIT_PID;
        }

        if (endId == (int64_t)pid)
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
    */

    int status;
    pid_t endID, childID = (pid_t)((int64_t)pid);
        
    time_t start = time(NULL);
    while(difftime(time(NULL), start) <= timeout_seconds)
    {
        endID = waitpid(childID, &status, WNOHANG|WUNTRACED);
        if (endID == -1) 
        {
            *detailed_error_code = errno;
            return FAIL_WAIT_PID;
        }
        else if (endID == 0) /* child still running         */
        {
              sleep(1);
        }
        else if (endID == childID)  /* child ended                 */
        {        
            if (WIFEXITED(status))
            {
                *exit_code = WEXITSTATUS(status);
                return SUCCESS;
            }
            
            if (WIFSIGNALED(status) || WIFSTOPPED(status))
            {
                *exit_code = WIFSIGNALED(status) || WIFSTOPPED(status);
                return FAIL_CHILD_PROCESS_FAILURE;
            }
            *exit_code = -1;
            return SUCCESS;
        }        
    }
   
    return FAIL_TIMEOUT;
}
