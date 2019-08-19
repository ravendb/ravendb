#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <Windows.h>

#include "rvn.h"
#include "status_codes.h"


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
rvn_wait_for_close_process(void* pid, int32_t timeout_ms, int32_t* exit_code, int32_t* detailed_error_code) {
    DWORD rc = WaitForSingleObject(pid, timeout_ms);
    
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
rvn_spawn_process(const char* filename, const char* cmdline, void* pid, void* stdin, void* stdout, void* stderr, int32_t* detailed_error_code) {
    int rc = SUCCESS;
    STARTUPINFO si;
    PROCESS_INFORMATION pi;
    SECURITY_ATTRIBUTES sa;

    HANDLE stdin_read = NULL;
    HANDLE stdin_write = NULL;
    HANDLE stdout_read = NULL;
    HANDLE stdout_write = NULL;
    HANDLE stderr_read = NULL;
    HANDLE stderr_write = NULL;

    ZeroMemory(&si, sizeof(STARTUPINFO));
    ZeroMemory(&pi, sizeof(PROCESS_INFORMATION));
    ZeroMemory(&sa, sizeof(SECURITY_ATTRIBUTES));

    if (!CreatePipe(&stdin_read, &stdin_write, &sa, 0)) {
        rc = FAIL_CREATE_PIPE;
        goto error;
    }
    if (!CreatePipe(&stdout_read, &stdout_write, &sa, 0)) {
        rc = FAIL_CREATE_PIPE;
        goto error;
    }
    if (!CreatePipe(&stderr_read, &stderr_write, &sa, 0)) {
        rc = FAIL_CREATE_PIPE;
        goto error;
    }

    if (!SetHandleInformation(stdout_read, HANDLE_FLAG_INHERIT, 0)) {
        rc = FAIL_FCNTL;
        goto error;
    }

    if (!SetHandleInformation(stderr_read, HANDLE_FLAG_INHERIT, 0)) {
        rc = FAIL_FCNTL;
        goto error;
    }

    if (!SetHandleInformation(stdin_write, HANDLE_FLAG_INHERIT, 0)) {
        rc = FAIL_FCNTL;
        goto error;
    }


    si.cb = sizeof(STARTUPINFO);
    si.hStdError = stderr_write;
    si.hStdOutput = stdout_write;
    si.hStdInput = stdin_read;
    si.dwFlags |= STARTF_USESTDHANDLES;

    sa.nLength = sizeof(SECURITY_ATTRIBUTES);
    sa.lpSecurityDescriptor = NULL;
    // Pipe handles are inherited
    sa.bInheritHandle = true;

    // Creates a child process
    if (!CreateProcess(
        filename,     // Module
        cmdline,                                       // Command-line
        NULL,                                       // Process security attributes
        NULL,                                       // Primary thread security attributes
        true,                                       // Handles are inherited
        CREATE_NEW_CONSOLE,                         // Creation flags
        NULL,                                       // Environment (use parent)
        NULL,                                       // Current directory (use parent)
        &si,                                        // STARTUPINFO pointer
        &pi                                         // PROCESS_INFORMATION pointer
    )) {
        rc = FAIL_CREATE_PROCESS;
        goto error;
    }

    goto success;

error:
    detailed_error_code = GetLastError();

    if (stdin_read != NULL)
        CloseHandle(stdin_read);
    if (stdin_write != NULL)
        CloseHandle(stdin_write);
    if (stdout_read != NULL)
        CloseHandle(stdout_read);
    if (stdout_write != NULL)
        CloseHandle(stdout_write);
    if (stderr_read != NULL)
        CloseHandle(stderr_read);
    if (stderr_write != NULL)
        CloseHandle(stderr_write);

success:

    *stdin = stdin_write;
    *stdout = stdout_read;
    *stderr = stderr_read;
    *pid = pi.hProcess;

    return 0;
}
