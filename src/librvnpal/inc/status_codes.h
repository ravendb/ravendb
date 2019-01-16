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
#define FAIL_INVALID_HANDLE             18
#define FAIL_TRUNCATE_FILE              19
#define FAIL_GET_FILE_SIZE              20
#define FAIL_ALLOC_FILE                 21
#define FAIL_READ_FILE                  22
#define FAIL_SET_FILE_POINTER           23
#define FAIL_SET_EOF                    24
#define FAIL_EOF                        25

#define ERRNO_SPECIAL_CODES_NONE        0
#define ERRNO_SPECIAL_CODES_ENOMEM      (1 << 0)
#define ERRNO_SPECIAL_CODES_ENOENT      (1 << 1)
#define ERRNO_SPECIAL_CODES_ENOSPC      (1 << 2)


#define SYNC_DIR_ALLOWED                0
#define SYNC_DIR_NOT_ALLOWED            1

#define MMOPTIONS_NONE                  0
#define MMOPTIONS_COPY_ON_WRITE         1

#define DELETE_ON_CLOSE_NO              0
#define DELETE_ON_CLOSE_YES             1

#define PROTECT_RANGE_NONE              0
#define PROTECT_RANGE_PROTECT           1
#define PROTECT_RANGE_UNPROTECT         2

#define JOURNAL_MODE_SAFE               0
#define JOURNAL_MODE_DANGER             1
#define JOURNAL_MODE_PURE_MEMORY        2

#endif
