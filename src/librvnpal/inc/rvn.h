#ifndef RVN_H
#define RVN_H

#include <stdint.h>


typedef int bool;
#define true 1
#define false 0

#if _WIN32
_declspec(dllexport)
#endif
uint64_t rvn_get_current_thread_id(void);

#if _WIN32
_declspec(dllexport)
#endif
int32_t rvn_write_header(const char *path, void *header, int32_t size, uint32_t *detailed_error_code);


#endif

