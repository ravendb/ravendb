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
#include <dirent.h>

#include "rvn.h"
#include "status_codes.h"
#include "internal_posix.h"

PRIVATE char** 
_parse_cmd_line(char* line, char *filename)
{
    int argc = 2;
    char** argv = malloc(sizeof(long) * argc);
    if (argv == NULL)
        return NULL;
    argv[0] = filename;
    argv[--argc] = NULL;    
    if (line == NULL || *line == '\0')
    {
        char **tmpargv = realloc(argv, sizeof(long)*(argc + 2));
        if (tmpargv == NULL)
        {
            free(argv);
            return NULL;
        }
        argv = tmpargv;        
        argv[1] = "";
        argv[2] = NULL;
        return argv;
    }
    
    char *rest = NULL;
    char *token;
    char *str = line;

    const char delimiters[] = " \t\n";    
    while((token = strtok_r(str, delimiters, &rest)))
    {   
        str = NULL;
        argv[argc++] = token;
        char **tmpargv = realloc(argv, sizeof(long)*(argc + 1));
        if (tmpargv == NULL)
        {
            free(argv);
            return NULL;
        }
        argv = tmpargv;
    }
    argv[argc] = NULL;
    return argv;
}

PRIVATE ssize_t 
_read_size_internal(int fd, void* buffer, size_t count)
{
    ssize_t result = read(fd, buffer, count);
    while ( result < 0 && errno == EINTR)
        result = read(fd, buffer, count);
    return result;
}



PRIVATE ssize_t 
_read_size(int fd, void* buffer, size_t count)
{
    ssize_t rc = 0;
    while (count > 0)
    {
        ssize_t result = _read_size_internal(fd, buffer, count);

        if (result <= 0)
            return -1;
        
        rc += result;
        buffer = (uint8_t*)buffer + result;
        count -= (size_t)result;
    }
    return rc;
}

PRIVATE void 
_close_if_open(int fd)
{
    if (fd >= 0)    
        close(fd);    
}

PRIVATE ssize_t
_write_size_internal(int fd, const void* buffer, size_t count)
{
    ssize_t result = write(fd, buffer, count);
    while ( result  < 0 && errno == EINTR)
        result = write(fd, buffer, count);
    return result;
}

PRIVATE ssize_t 
_write_size(int fd, const void* buffer, size_t count)
{
    ssize_t rc = 0;
    while (count > 0)
    {
        ssize_t result = _write_size_internal(fd, buffer, count);

        if (result <= 0)
            return -1;
        
        rc += result;
        buffer = (const uint8_t*)buffer + result;
        count -= (size_t)result;
    }
    return rc;
}

PRIVATE void 
_exit_child(int pipeToParent, int error)
{
    if (pipeToParent != -1)
        _write_size(pipeToParent, &error, sizeof(error));

    _exit(error != 0 ? error : EXIT_FAILURE);
}

PRIVATE int32_t
_redirect_std(int fd, int fileno)
{
    int32_t result = dup2(fd, fileno);
    while ( result < 0 && errno == EINTR)
        result = dup2(fd, fileno);
    return result;
}

PRIVATE bool
_is_file_exists_in_working_dir(const char *filename)
{    
    struct dirent *dir;
    DIR *d = opendir(".");
    bool found = false;
    if (d)
    {
        while ((dir = readdir(d)) != NULL)
        {
            if (strcmp(filename, dir->d_name) == 0)
            {
                found = true;
                break;
            }
        }
        closedir(d);
    }

    return found;
}

EXPORT int32_t
rvn_spawn_process(const char* filename, char* cmdline, void** pid, void** standard_in, void** standard_out, int32_t* detailed_error_code) {    
    int stdinFds[2] = {-1, -1}, stdoutFds[2] = {-1, -1}, waitForChildToExecPipe[2] = {-1, -1};
    pid_t processId = -1;
    int thread_cancel_state;
    sigset_t signal_set;
    sigset_t old_signal_set;

    char *modified = malloc(strlen(filename) + 3);
    if (modified == NULL)
    {
        *detailed_error_code = errno;
        return FAIL_NOMEM;
    }
    *modified = '\0';
    if (filename[0] != '/')
    {
        if (_is_file_exists_in_working_dir(filename) == true)
            strcpy(modified, "./");
    }
    strcat(modified, (char *)filename);

    int32_t rc = SUCCESS;
    
    char **argv = _parse_cmd_line(cmdline, modified);
    if (argv == NULL)
    {
        *detailed_error_code = errno;
        free(modified);
        return FAIL_NOMEM;
    }

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
        if ( (result = _redirect_std(stdoutFds[1], STDOUT_FILENO)) == -1 ||
             (result = _redirect_std(stdoutFds[1], STDERR_FILENO)) == -1 ||
             (result = _redirect_std(stdinFds[1],  STDIN_FILENO)) == -1 )
        {
            _exit_child(waitForChildToExecPipe[1], errno);
        }        
        
        execvp(modified, argv);
        _exit_child(waitForChildToExecPipe[1], errno);
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
    if (argv != NULL)
        free(argv); /* safe because vfork returns only after exec started a new process */
        
    *detailed_error_code = errno;
    _close_if_open(stdinFds[0]);
    _close_if_open(stdoutFds[1]);
    _close_if_open(waitForChildToExecPipe[1]);
   
    if (waitForChildToExecPipe[0] != -1)
    {
        int childError;
        if (rc == SUCCESS)
        {
            ssize_t result = _read_size(waitForChildToExecPipe[0], &childError, sizeof(childError));
            if (result == sizeof(childError))
            {
                rc = FAIL_CHILD_PROCESS_FAILURE;
                *detailed_error_code = childError;
            }
        }
        _close_if_open(waitForChildToExecPipe[0]);
    }

    if (rc != SUCCESS)
    {
        _close_if_open(stdinFds[1]);
        _close_if_open(stdoutFds[0]);
        
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
    /* calling this to observe the status code of the process*/
    waitpid((pid_t)(int64_t)(int *)pid, &rc, WNOHANG | WUNTRACED);
    return SUCCESS;
}

EXPORT int32_t
rvn_wait_for_close_process(void* pid, int32_t closewait_timeout_seconds, int32_t* exit_code, int32_t* detailed_error_code) {
    int status;
    pid_t endID, childID = (pid_t)((int64_t)pid);
        
    time_t start = time(NULL);
    while(difftime(time(NULL), start) <= closewait_timeout_seconds)
    {
        endID = waitpid(childID, &status, WNOHANG | WUNTRACED);
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
                *exit_code = WIFSIGNALED(status) | WIFSTOPPED(status);
                return FAIL_CHILD_PROCESS_FAILURE;
            }
            *exit_code = -1;
            return SUCCESS;
        }        
    }
   
    return FAIL_TIMEOUT;
}
