#if defined(__APPLE__)

#include <sys/param.h>
#include <sys/mount.h>

#elif defined(__unix__)

#include <sys/statfs.h>

#endif


int32_t
rvn_allocate_file_space(int32_t fd, int64_t size, int32_t *detailed_error_code);


int64_t
rvn_pwrite(int32_t fd, void *buffer, uint64_t count, uint64_t offset, int32_t *detailed_error_code);
