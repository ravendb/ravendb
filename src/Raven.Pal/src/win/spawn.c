#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <Windows.h>
#include "rvn.h"
#include "status_codes.h"
#include <stdio.h>



EXPORT int32_t
rvn_kill_process(void* pid, int32_t* detailed_error_code) {
    if (!TerminateProcess(pid, 9)) {
        *detailed_error_code = GetLastError();
        return FAIL_KILL;
    }
    CloseHandle(pid);
    return SUCCESS;
}

EXPORT int32_t
rvn_wait_for_close_process(void* pid, int32_t timeout_seconds, int32_t* exit_code, int32_t* detailed_error_code) {
    DWORD rc = WaitForSingleObject(pid, timeout_seconds * 1000);
    
    if (rc == WAIT_TIMEOUT)
    {
        *detailed_error_code = GetLastError();
        return FAIL_TIMEOUT;
    }

    if (!GetExitCodeProcess(pid, exit_code)) {
        *detailed_error_code = GetLastError();
        return FAIL_WAIT_PID;
    }

    CloseHandle(pid);

    return SUCCESS;
}

EXPORT int32_t
rvn_spawn_process(const char* filename, char* cmdline, void** pid, void** standard_in, void** standard_out, int32_t* detailed_error_code) {    
    int rc = SUCCESS;
    STARTUPINFO si;
    PROCESS_INFORMATION pi;
    SECURITY_ATTRIBUTES sa;

    HANDLE stdin_read = INVALID_HANDLE_VALUE;
    HANDLE stdin_write = INVALID_HANDLE_VALUE;
    HANDLE stdout_read = INVALID_HANDLE_VALUE;
    HANDLE stdout_write = INVALID_HANDLE_VALUE;

    int filename_len = strlen(filename);
    int cmdline_len = strlen(cmdline);
    int line_len = filename_len + cmdline_len + 2;// space + null
    ZeroMemory(&si, sizeof(STARTUPINFO));
    ZeroMemory(&pi, sizeof(PROCESS_INFORMATION));
    ZeroMemory(&sa, sizeof(SECURITY_ATTRIBUTES));

    sa.nLength = sizeof(SECURITY_ATTRIBUTES);
    sa.lpSecurityDescriptor = NULL;
    /* Pipe handles are inherited*/
    sa.bInheritHandle = true;

    char* line = malloc(line_len); 
    if(line == NULL)
    {
        rc = FAIL_NOMEM;
        goto error;
    }
    memcpy(line, filename, filename_len);
    line[filename_len] = ' ';
    memcpy(line + filename_len + 1, cmdline, cmdline_len);
    line[line_len - 1] = '\0';

    if (!CreatePipe(&stdin_read, &stdin_write, &sa, 0)) {
        rc = FAIL_CREATE_PIPE;
        goto error;
    }
    if (!CreatePipe(&stdout_read, &stdout_write, &sa, 0)) {
        rc = FAIL_CREATE_PIPE;
        goto error;
    }

    if (!SetHandleInformation(stdout_read, HANDLE_FLAG_INHERIT, 0)) {
        rc = FAIL_FCNTL;
        goto error;
    }

    if (!SetHandleInformation(stdin_write, HANDLE_FLAG_INHERIT, 0)) {
        rc = FAIL_FCNTL;
        goto error;
    }


    si.cb = sizeof(STARTUPINFO);
    si.hStdError = stdout_write;
    si.hStdOutput = stdout_write;
    si.hStdInput = stdin_read;
    si.dwFlags |= STARTF_USESTDHANDLES;

    /* Creates a child process*/
    if (!CreateProcess(
        NULL,     /* Module*/
        line,                                    /* Command-line*/
        NULL,                                       /* Process security attributes*/
        NULL,                                       /* Primary thread security attributes*/
        true,                                       /* Handles are inherited*/
        CREATE_NEW_CONSOLE,                         /* Creation flags*/
        NULL,                                       /* Environment (use parent)*/
        NULL,                                       /* Current directory (use parent)*/
        &si,                                        /* STARTUPINFO pointer*/
        &pi                                         /* PROCESS_INFORMATION pointer*/
    )) {
        rc = FAIL_CREATE_PROCESS;
        goto error;
    }

    *standard_in = stdin_write;
    *standard_out = stdout_read;
    *pid = pi.hProcess;

    goto success;

error:
    *detailed_error_code = GetLastError();
    if (stdin_write != INVALID_HANDLE_VALUE)
        CloseHandle(stdin_write);
    if (stdout_read != INVALID_HANDLE_VALUE)
        CloseHandle(stdout_read);

success:
    if (stdin_read != INVALID_HANDLE_VALUE)
        CloseHandle(stdin_read);
    if (stdout_write != INVALID_HANDLE_VALUE)
        CloseHandle(stdout_write);

    if(line != NULL)
        free(line);
    if (pi.hThread != INVALID_HANDLE_VALUE)
        CloseHandle(pi.hThread);

    return rc;
}
