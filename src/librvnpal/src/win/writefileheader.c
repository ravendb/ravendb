#include <stdint.h>
#include <windows.h>

#include "rvn.h"
#include "status_codes.h"
#include "internal_win.h"

EXPORT int32_t
rvn_write_header (const char *path, void *header, int32_t size,
		  int32_t * detailed_error_code) 
{
	int32_t rc;
	HANDLE file = CreateFile(path,
		GENERIC_WRITE | GENERIC_READ,
		FILE_SHARE_READ,
		NULL,
		CREATE_ALWAYS,
		FILE_ATTRIBUTE_NORMAL,
		NULL
		);

	if(file == INVALID_HANDLE_VALUE) {
		rc = FAIL_OPEN_FILE;
		goto error_cleanup;
	}

	if ( !WriteFile(file, header, size, NULL, NULL) ){
		rc = FAIL_WRITE_FILE;
		goto error_cleanup;	
	}

	if ( !FlushFileBuffers(file) ){
		rc = FAIL_SYNC_FILE;
		goto error_cleanup;
	}

	CloseHandle(file);

	return SUCCESS;

error_cleanup:
	*detailed_error_code = GetLastError();

	if(file != INVALID_HANDLE_VALUE){
		CloseHandle(file);
	}

	return rc;

}
