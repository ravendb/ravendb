#include <stdint.h>
#include <windows.h>

#include "rvn.h"
#include "status_codes.h"
#include "internal_win.h"

EXPORT int32_t 
rvn_get_error_string(int32_t error, char* buf, int32_t buf_size, int32_t* special_errno_flags) {
	
	switch (error) {
		case ERROR_NOT_ENOUGH_MEMORY:
			*special_errno_flags = ERRNO_SPECIAL_CODES_ENOMEM;
			break;
		case ERROR_FILE_NOT_FOUND:
			*special_errno_flags = ERRNO_SPECIAL_CODES_ENOENT;
			break;
		default:
			*special_errno_flags = ERRNO_SPECIAL_CODES_NONE;
			break;
	}
	
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

