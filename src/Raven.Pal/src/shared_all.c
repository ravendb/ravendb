#include <sys/types.h>
#include <string.h>
#include <stdlib.h>

#include "rvn.h"
#include "rvn_internal.h"
#include "status_codes.h"


void noop_void(int64_t size, char* filename){}
bool noop_bool(int64_t size, char* filename){ return false; }


MemoryLockCallback g_locked_memory_callback = noop_void;
RecoveryMemoryLockFailureCallback g_recovery_memory_lock_failure_callback = noop_bool;

EXPORT
void rvn_register_callbacks(MemoryLockCallback memoryLockCallback, 
    RecoveryMemoryLockFailureCallback recoveryMemoryLockFailureCallback)
{
    g_locked_memory_callback = memoryLockCallback;
    g_recovery_memory_lock_failure_callback = recoveryMemoryLockFailureCallback;
}

PRIVATE int64_t
_nearest_size_to_page_size(int64_t orig_size, int64_t sys_page_size)
{
    int64_t mod = orig_size % sys_page_size;
    if (mod == 0)
    {
        return orig_size;
    }
    return ((orig_size / sys_page_size) + 1) * sys_page_size;
}

rvn_write_mode _get_writer_mode()
{
    const char* env_var_name = "RAVEN_WRITER_MODE";
#if defined(_WIN32)
    char buffer[32];
    memset(buffer, 0, sizeof(buffer));
    size_t required_size;
    if (getenv_s(&required_size, buffer, sizeof buffer, env_var_name) != 0 || 
        required_size == 0)
    {
        return rvn_mode_default;
    }
#else
    const char* buffer = getenv(env_var_name);
    if (buffer == NULL)
    {
        return rvn_mode_default;
    }
#endif

     if (strcmp(buffer, "vectored_file_io") == 0)
    {
        return rvn_write_mode_vectored_file_io;
    }
    else if (strcmp(buffer, "file_io") == 0)
    {
        return rvn_write_mode_file_io;
    }
    else if (strcmp(buffer, "io_ring") == 0)
    {
        return rvn_write_mode_io_ring;
    }
    else if (strcmp(buffer, "mmap") == 0)
    {
        return rvn_write_mode_mmap;
    }
    else // Invalid value
    {
        return rvn_mode_default;
    }
}

