#include <sys/types.h>

#include "rvn.h"
#include "status_codes.h"


void noop(int64_t size, char* filename){}

MemoryLockCallback g_locked_memory_callback = noop;

EXPORT
void rvn_register_callback(MemoryLockCallback callback)
{
    g_locked_memory_callback = callback;
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
