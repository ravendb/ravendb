#ifndef __STATUS_CODES_H
#define __STATUS_CODES_H

#ifndef CIFS_MAGIC_NUMBER
#define CIFS_MAGIC_NUMBER               0xff534d42 /* TODO: Check if this is correct through out all plats */
#endif

#define SUCCESS                          0
#define FAIL                            -1
#define FAIL_OPEN_FILE                   1
#define FAIL_SEEK_FILE                   2
#define FAIL_WRITE_FILE                  3
#define FAIL_SYNC_FILE                   4
#define FAIL_NOMEM                       5
#define FAIL_STAT_FILE                   6
#define FAIL_RACE_RETRIES                7
#define FAIL_PATH_RECURSION              8
#define FAIL_FLUSH_FILE                  9
#define FAIL_SYSCONF                    10
#define FAIL_PWRITE                     11
#define FAIL_PWRITE_WITH_RETRIES        12
#define FAIL_MMAP64                     13
#define FAIL_UNLINK                     14
#define FAIL_CLOSE                      15
#define FAIL_ALLOCATION_NO_RESIZE       16
#define FAIL_FREE                       17

#define ERRNO_SPECIAL_CODES_NO_ERROR    (1 << 0)
#define ERRNO_SPECIAL_CODES_ENOMEM      (1 << 1)
#define ERRNO_SPECIAL_CODES_ENOENT      (1 << 2)
#define ERRNO_SPECIAL_CODES_ENOSPC      (1 << 3)

#define SYNC_DIR_ALLOWED                0
#define SYNC_DIR_NOT_ALLOWED            1

#define MMOPTIONS_NONE                  0
#define MMOPTIONS_COPY_ON_WRITE         1

#define MSYNC_OPTIONS_MS_SYNC           (1 << 0)
#define MSYNC_OPTIONS_MS_ASYNC          (1 << 1)
#define MSYNC_OPTIONS_MS_INVALIDATE     (1 << 2)

#define MPROTECT_OPTIONS_PROT_NONE      0
#define MPROTECT_OPTIONS_PROT_READ      (1 << 0)
#define MPROTECT_OPTIONS_PROT_WRITE     (1 << 1)
#define MPROTECT_OPTIONS_PROT_EXEC      (1 << 2)
#define MPROTECT_OPTIONS_PROT_GROWSUP   (1 << 3)
#define MPROTECT_OPTIONS_PROT_GROWSDOWN (1 << 4)

#endif
