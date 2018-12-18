#ifndef __POSIX_ENUMS_H
#define __POSIX_ENUMS_H

#ifndef CIFS_MAGIC_NUMBER
#define CIFS_MAGIC_NUMBER     0xff534d42 /* TODO: Check if this is correct through out all plats */
#endif

#define SUCCESS 		0
#define FAIL_OPEN_FILE 		1
#define FAIL_SEEK_FILE 		2
#define FAIL_WRITE_FILE 	3
#define FAIL_SYNC_FILE 		4
#define FAIL_NOMEM 		5
#define FAIL_STAT_FILE 		6
#define FAIL_RACE_RETRIES 	7
#define FAIL_PATH_RECURSION	8
#define FAIL_FLUSH_FILE		9

#define ERRNO_SPECIAL_CODES_NONE	1
#define ERRNO_SPECIAL_CODES_ENOMEM	2
#define ERRNO_SPECIAL_CODES_ENOENT	4            

#define SYNC_DIR_FAILED		-1
#define SYNC_DIR_ALLOWED 	0
#define SYNC_DIR_NOT_ALLOWED	1

#endif
