#ifndef RVN_H
#define RVN_H

#include <stdint.h>

#ifdef __APPLE__
#define EXPORT __attribute__((visibility("default")))	
#elif _WIN32
#define EXPORT _declspec(dllexport) 
#else
#define EXPORT 
#endif

typedef int bool;
#define true 1
#define false 0

EXPORT uint64_t
rvn_get_current_thread_id(void);

EXPORT int32_t
rvn_write_header(const char *path, void *header, int32_t size, uint32_t *detailed_error_code);


EXPORT int32_t
sync_directory_for (const char *file_path, uint32_t * detailed_error_code);


int32_t /* internal use */
flush_file (int32_t fd);

int32_t /* internal use */
sync_directory_allowed (int dir_fd);


#endif

