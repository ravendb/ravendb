#if defined(__unix__) && !defined(APPLE)

#define _GNU_SOURCE
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <stdlib.h>

#include "rvn.h"
#include "posixenums.h"

int32_t rvn_get_error_string(int32_t error, char* buf, int32_t buf_size, int32_t* special_errno_flags)
{
	char* tmp_buf = NULL;
	/* strerror_r returns (in GNU-specific) the string either to buf (with max buf_size) OR to the char* rc. */
	switch (error)
	{
		case ENOMEM:
			*special_errno_flags = ERRNO_SPECIAL_CODES_ENOMEM;
			break;
		case ENOENT:
			*special_errno_flags = ERRNO_SPECIAL_CODES_ENOENT;
			break;
		default:
			*special_errno_flags = ERRNO_SPECIAL_CODES_NONE;
			break;
	}
	
	tmp_buf = malloc(buf_size);
	if(tmp_buf == NULL)
		goto error_cleanup;

	char* err = strerror_r(error, tmp_buf, buf_size);
	if(err == NULL)
		goto error_cleanup;

	size_t size = strlen(err);
	
	size_t actual_size = size >  buf_size-1 ? buf_size-1 : size;
	memcpy(buf, err, actual_size);

	buf[actual_size] = 0;
	free(tmp_buf);

	return actual_size;


error_cleanup:
	if(tmp_buf != NULL)
		free(tmp_buf);
	return -1;
}

#endif
