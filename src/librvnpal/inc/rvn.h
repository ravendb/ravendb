#ifndef RVN_H
#define RVN_H

#include <stdint.h>

#if _WIN32
_declspec(dllexport)
#endif
int32_t rvn_memcmp(const void *s1, const void *s2, int32_t n);

#if _WIN32
_declspec(dllexport)
#endif
uint64_t rvn_get_current_thread_id(void);

#if _WIN32
_declspec(dllexport)
#endif
int32_t rvn_write_header(const char *path, void *header, int32_t size, uint32_t *detailed_error_code);


#endif

