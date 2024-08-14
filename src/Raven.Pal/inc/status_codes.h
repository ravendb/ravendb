#ifndef __STATUS_CODES_H
#define __STATUS_CODES_H

#ifndef CIFS_MAGIC_NUMBER
#define CIFS_MAGIC_NUMBER               0xff534d42 /* TODO: Check if this is correct through out all plats */
#endif
enum
{
    SUCCESS                     =       0,
    FAIL                        =      -1,
    FAIL_OPEN_FILE              =       1,
    FAIL_SEEK_FILE              =       2,
    FAIL_WRITE_FILE             =       3,
    FAIL_SYNC_FILE              =       4,
    FAIL_NOMEM                  =       5,
    FAIL_STAT_FILE              =       6,
    FAIL_RACE_RETRIES           =       7,
    FAIL_PATH_RECURSION         =       8,
    FAIL_FLUSH_FILE             =       9,
    FAIL_SYSCONF                =      10,
    FAIL_PWRITE                 =      11,
    FAIL_PWRITE_WITH_RETRIES    =      12,
    FAIL_MMAP64                 =      13,
    FAIL_UNLINK                 =      14,
    FAIL_CLOSE                  =      15,
    FAIL_ALLOCATION_NO_RESIZE   =      16,
    FAIL_FREE                   =      17,
    FAIL_INVALID_HANDLE         =      18,
    FAIL_TRUNCATE_FILE          =      19,
    FAIL_GET_FILE_SIZE          =      20,
    FAIL_ALLOC_FILE             =      21,
    FAIL_READ_FILE              =      22,
    FAIL_SET_FILE_POINTER       =      23,
    FAIL_SET_EOF                =      24,
    FAIL_EOF                    =      25,
    FAIL_PREFETCH               =      26,
    FAIL_CALLOC                 =      27,
    FAIL_TEST_DURABILITY        =      28,
    FAIL_CREATE_DIRECTORY       =      29,
    FAIL_NOT_DIRECTORY          =      30,
    FAIL_BROKEN_LINK            =      31,
    FAIL_GET_REAL_PATH          =      32,
    FAIL_GET_MODULE_HANDLE      =      33,
    FAIL_DISCARD_VIRTUAL_MEMORY =      34,
    FAIL_LOCK_MEMORY            =      35,
    FAIL_MAP_VIEW_OF_FILE       =      36,
    FAIL_UNMAP_VIEW_OF_FILE     =      37,
    FAIL_DUPLICATE_HANDLE       =      38,
    FAIL_SIZE_INVALID_32_BITS   =      39,
    FAIL_SIZE_NEGATIVE_OR_ZERO  =      40,
    FAIL_SPARSE_NOT_SUPPORTED   =      41, 
    FAIL_GET_VOLUME_DETAILS     =      42,
    FAIL_SET_SPARSE             =      43,
    FAIL_SET_SPARSE_RANGE       =      44,
    
};
#define ERRNO_SPECIAL_CODES_NONE        0
#define ERRNO_SPECIAL_CODES_ENOMEM      (1 << 0)
#define ERRNO_SPECIAL_CODES_ENOENT      (1 << 1)
#define ERRNO_SPECIAL_CODES_ENOSPC      (1 << 2)


#define SYNC_DIR_ALLOWED                0
#define SYNC_DIR_NOT_ALLOWED            1

#define MMOPTIONS_NONE                  0
#define MMOPTIONS_COPY_ON_WRITE         (1 << 0)
#define MMOPTIONS_DELETE_ON_CLOSE       (1 << 1)

#define FILE_CLOSE_OPT_NONE             0
#define FILE_CLOSE_OPT_DELETE_ON_CLOSE  1

#define PROTECT_RANGE_NONE              0
#define PROTECT_RANGE_PROTECT           1
#define PROTECT_RANGE_UNPROTECT         2

#define JOURNAL_MODE_SAFE               0
#define JOURNAL_MODE_DANGER             1
#define JOURNAL_MODE_PURE_MEMORY        2

#define DURABILITY_NONE                 0
#define DURABILITY_SUPPORTED            1
#define DURABILITY_NOT_SUPPORTED        2

#endif
