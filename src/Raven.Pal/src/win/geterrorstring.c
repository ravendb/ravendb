#include <stdint.h>
#include <Windows.h>

#include "rvn.h"
#include "status_codes.h"
#include "internal_win.h"

EXPORT int32_t
rvn_get_error_meaning(int32_t error)
{
	switch (error) {
		case ERROR_NOT_ENOUGH_MEMORY:
		case ERROR_OUTOFMEMORY:
		case ERROR_COMMITMENT_MINIMUM:
			return ERRNO_SPECIAL_CODES_ENOMEM;
		case ERROR_FILE_NOT_FOUND:
			return ERRNO_SPECIAL_CODES_ENOENT;
        case ERROR_DISK_FULL:
            return ERRNO_SPECIAL_CODES_ENOSPC;
		default:
			return ERRNO_SPECIAL_CODES_NONE;
	}
}

EXPORT int32_t 
rvn_get_error_string(int32_t error, char* buf, int32_t buf_size, int32_t* special_errno_flags) {
	
	*special_errno_flags = rvn_get_error_meaning(error);
	
	DWORD rc = FormatMessageA( // intentionally using A for ASCII here  
		FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS,
		NULL,
		error,
		MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),
		buf,
		buf_size,
		NULL
	);

	if(rc == 0) 
		return -1;
	
	return rc;

}

