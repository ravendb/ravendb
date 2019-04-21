#include <stdint.h>
#include <Windows.h>
#include <VersionHelpers.h>

#include "rvn.h"
#include "status_codes.h"
#include "internal_win.h"

int32_t
rvn_get_system_information(struct SYSTEM_INFORMATION *sys_info, int32_t *detailed_error_code)
{
    sys_info->page_size = 64 * 1024;
    sys_info->prefetch_status = IsWindows8OrGreater() ? 1 : 0;

    *detailed_error_code = 0;

    return SUCCESS;
}
