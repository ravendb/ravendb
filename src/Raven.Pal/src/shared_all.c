#include <sys/types.h>
#include <string.h>
#include <stdlib.h>

#include "rvn.h"
#include "rvn_internal.h"
#include "status_codes.h"

void noop_void(int64_t size, char *filename) {}
bool noop_bool(int64_t size, char *filename) { return false; }

struct rvn_configuration g_cfg = {
    .version = none,
    .io_ring_queue_size = -1,
    .write_mode = rvn_mode_default};

EXPORT
int32_t rvn_startup_configure(struct rvn_configuration *cfg, int32_t *detailed_error_code)
{
    // here we assume that there can be _no_ concurrency at all
    // and that this is called on process startup and never again
    if (cfg == NULL)
    {
        return FAIL_INVALID_CONFIGURATION;
    }
    if (g_cfg.version != 0)
    {
        return FAIL_ALREADY_CONFIGURED;
    }
    cfg->pal_version = rvn_get_pal_ver();
    g_cfg = *cfg;
    if (!g_cfg.memoryLockCallback)
        g_cfg.memoryLockCallback = noop_void;
    if (!g_cfg.recoveryMemoryLockFailureCallback)
        g_cfg.recoveryMemoryLockFailureCallback = noop_bool;

    int32_t rc = rvn_one_time_init(detailed_error_code);
    cfg->write_mode = g_cfg.write_mode;
    return rc;
}

EXPORT int32_t
rvn_ensure_hard_link_non_durable(const char *src, const char *dst, int32_t *detailed_error_code)
{
    char is_same = false;
    int32_t rc = rvn_is_same_hard_link(src, dst, &is_same, detailed_error_code);
    if (rc != SUCCESS)
        return rc;
    if (is_same)
        return SUCCESS;
    return rvn_hard_link_non_durable(src, dst, detailed_error_code);
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

PRIVATE
uint64_t nextPowerOf2(uint64_t n)
{
    if (n == 0)
    {
        return 1;
    }

    n--;
    n |= n >> 1;
    n |= n >> 2;
    n |= n >> 4;
    n |= n >> 8;
    n |= n >> 16;
    n |= n >> 32;

    return n + 1;
}
