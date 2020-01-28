#include <sys/types.h>

#include "rvn.h"
#include "status_codes.h"

PRIVATE int64_t
_nearest_size_to_page_size(int64_t orig_size, int64_t allocation_granularity)
{
    int64_t mod = orig_size % allocation_granularity;
    if (mod == 0)
    {
        return rvn_max(orig_size, allocation_granularity);
    }
    return ((orig_size / allocation_granularity) + 1) * allocation_granularity;
}
