#include <sys/types.h>

#include "rvn.h"
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