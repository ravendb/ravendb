#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include <libgen.h>
#include <sys/mman.h>
#include <stdio.h>

#include "rvn.h"
#include "internal_posix.h"
#include "status_codes.h"


PRIVATE int32_t
_sync_directory_for_internal(char *dir_path, int32_t *detailed_error_code)
{
    int32_t rc;
    int32_t fd = open(dir_path, 0, 0);
    if (fd == -1)
    {
        rc = FAIL_OPEN_FILE;
        goto error_cleanup;
    }

    rc = _sync_directory_allowed(fd);

    if (rc == FAIL)
    {
        goto error_cleanup;
    }

    if (rc == SYNC_DIR_NOT_ALLOWED)
    {
        rc = SUCCESS;
        goto error_cleanup;
    }

    if (_flush_file(fd) == -1)
    {
        rc = FAIL_FLUSH_FILE;
        goto error_cleanup;
    }

    rc = SUCCESS;
    goto cleanup;

error_cleanup:
    *detailed_error_code = errno;
cleanup:
    if (fd != -1)
        close(fd);

    return rc;
}



int32_t
_sync_directory_maybe_symblink(char *dir_path, int32_t *detailed_error_code)
{
    int32_t rc;
    char* real_path = realpath(dir_path, NULL);
    if (real_path == NULL)
    {
        rc = FAIL_GET_REAL_PATH;
        goto error_cleanup;
    }

    rc = _sync_directory_for_internal(real_path, detailed_error_code);
    if (rc != SUCCESS)
        goto error_cleanup;

    rc = SUCCESS;
    goto cleanup;

error_cleanup:
    *detailed_error_code = errno;
cleanup:
    if (real_path != NULL)
        free(real_path);

    return rc;
}

PRIVATE int32_t
_sync_directory_for(const char *file_path, int32_t *detailed_error_code)
{
    assert(file_path != NULL);

    char *file_path_copy = NULL;
    file_path_copy = strdup(file_path);
    if (file_path_copy == NULL)
    {
        *detailed_error_code = errno;
        return FAIL_NOMEM;
    }
    char *dir_path = dirname(file_path_copy);

    int32_t rc = _sync_directory_maybe_symblink(dir_path, detailed_error_code);

    free(file_path_copy);

    return rc;
}
