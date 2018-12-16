#if defined(__unix__) && !defined(APPLE)

#define _GNU_SOURCE
#include <unistd.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <sys/statfs.h>
#include <sys/vfs.h>
#include <libgen.h>
#include <linux/magic.h>
#include <string.h>
#include <assert.h>
#include <limits.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>

#include <rvn.h>

#ifndef CIFS_MAGIC_NUMBER
#define CIFS_MAGIC_NUMBER     0xff534d42
#endif

#define SUCCESS 			0
#define FAIL_OPEN_FILE 		1
#define FAIL_SEEK_FILE 		2
#define FAIL_WRITE_FILE 	3
#define FAIL_SYNC_FILE 		4
#define FAIL_NOMEM 			5
#define FAIL_STAT_FILE 		6
#define FAIL_RACE_RETRIES 	7
#define FAIL_PATH_RECURSION 8
#define FAIL_FLUSH_FILE		9

#define SYNC_DIR_FAILED			-1
#define SYNC_DIR_ALLOWED 		0
#define SYNC_DIR_NOT_ALLOWED	1


static int32_t
flush_file (int32_t fd)
{
  /* fcntl(fd, F_FULLFSYNC); */
  return fsync (fd);
}

static int32_t
sync_directory_allowed (int dir_fd)
{
  struct statfs buf;
  if (fstatfs (dir_fd, &buf) == -1)
    return SYNC_DIR_FAILED;

  switch (buf.f_type)
    {
    case NFS_SUPER_MAGIC:
    case CIFS_MAGIC_NUMBER:
    case SMB_SUPER_MAGIC:
      return SYNC_DIR_ALLOWED;
    default:
      return SYNC_DIR_NOT_ALLOWED;
    }
}


static int32_t
sync_directory_for_internal (char *dir_path, uint32_t * detailed_error_code)
{
  int rc;
  int fd = open (dir_path, 0, 0);
  if (fd == -1)
    {
      rc = FAIL_OPEN_FILE;
      goto error_cleanup;
    }

  rc = sync_directory_allowed (fd);

  if (rc == SYNC_DIR_FAILED)
    {
      goto error_cleanup;
    }

  if (rc == SYNC_DIR_NOT_ALLOWED)
    {
      rc = SUCCESS;
      goto error_cleanup;
    }

  if (flush_file (fd) == -1)
    {
      rc = FAIL_FLUSH_FILE;
      goto error_cleanup;
    }

  rc = SUCCESS;

error_cleanup:
  *detailed_error_code = errno;
  if (fd != -1)
    close (fd);

  return rc;
}

static int32_t
sync_directory_maybe_symblink (char *dir_path, int depth,
			       uint32_t * detailed_error_code)
{
  struct stat sb;
  char *link_name = NULL;
  int rc;

  int steps = 10;

  while (1)
    {
      if (lstat (dir_path, &sb) == -1)
	{
	  rc = FAIL_STAT_FILE;
	  goto error_cleanup;
	}

      link_name = malloc (sb.st_size + 1);
      if (link_name == NULL)
	{
	  rc = FAIL_NOMEM;
	  goto error_cleanup;
	}

      int len = readlink (dir_path, link_name, sb.st_size + 1);

      if (len == 0 || (len == -1 && errno == EINVAL))	/* EINVAL on non-symlink dir_path */
	{
	  rc = sync_directory_for_internal (dir_path, detailed_error_code);
	  goto success;
	}

      if (len < 0)
	{
	  rc = FAIL_STAT_FILE;
	  goto error_cleanup;
	}

      if (len > sb.st_size)
	{
	  /* race: the link has changed, re-read */
	  free (link_name);
	  link_name = NULL;
	  if (steps-- > 0)
	    continue;
	  rc = FAIL_RACE_RETRIES;
	  goto error_cleanup;
	}

      link_name[len] = '\0';
      break;
    }

  if (depth == 0)
    {
      rc = FAIL_PATH_RECURSION;
      goto error_cleanup;
    }

  rc =
    sync_directory_maybe_symblink (link_name, depth - 1, detailed_error_code);

error_cleanup:
  *detailed_error_code = errno;
success:
  if (link_name != NULL)
    free (link_name);

  return rc;
}

int32_t
sync_directory_for (const char *file_path, uint32_t * detailed_error_code)
{
  assert (file_path != NULL);

  char *file_path_copy = NULL;
  file_path_copy = strdup (file_path);
  if (file_path_copy == NULL)
    {
      *detailed_error_code = errno;
      return FAIL_NOMEM;
    }
  char *dir_path = dirname (file_path_copy);

  int32_t rc =
    sync_directory_maybe_symblink (dir_path, 4096, detailed_error_code);

  free (file_path_copy);

  return rc;
}

int32_t
rvn_write_header (const char *path, void *header, int32_t size,
		  uint32_t * detailed_error_code)
{
  int32_t rc;
  bool syncIsNeeded = false;
  int32_t fd = open (path, O_WRONLY | O_CREAT, S_IWUSR | S_IRUSR);

  if (fd == -1)
    {
      rc = FAIL_OPEN_FILE;
      goto error_cleanup;
    }

  int32_t remaining = size;

  int64_t sz = lseek (fd, 0L, SEEK_END);
  if (sz == -1)
    {
      rc = FAIL_SEEK_FILE;
      goto error_cleanup;
    }

  if (lseek (fd, 0L, SEEK_SET) == -1)
    {
      rc = FAIL_SEEK_FILE;
      goto error_cleanup;
    }
  if (sz != remaining)
    syncIsNeeded = true;

  while (remaining > 0)
    {
      uint64_t written = write (fd, header, (uint64_t) remaining);
      if (written == -1)
	{
	  rc = FAIL_WRITE_FILE;
	  goto error_cleanup;
	}

      remaining -= (int) written;
      header += written;
    }
  if (flush_file (fd) == -1)
    {
      rc = FAIL_FLUSH_FILE;
      goto error_cleanup;
    }

  close (fd);
  fd = -1;

  if (syncIsNeeded == true)
    return sync_directory_for (path, detailed_error_code);
  return SUCCESS;

error_cleanup:

  *detailed_error_code = errno;
  if (fd != -1)
    close (fd);

  return rc;
}

#endif
