#include <sys/types.h>
#include <stdatomic.h>

#include "rvn.h"
#include "status_codes.h"


_Atomic int64_t g_locked_memory_size;

EXPORT
_Atomic int64_t* get_locked_memory_size()
{
    return &g_locked_memory_size;
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