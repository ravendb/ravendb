#ifndef INTERNALPOSIX_H
#define INTERNALPOSIX_H

#ifdef __APPLE__
#define rvn_mmap mmap
#define rvn_ftruncate ftruncate
#define rvn_pread pread
#define O_DIRECT 0
#define O_LARGEFILE 0
#else
#define rvn_mmap mmap64
#define rvn_ftruncate ftruncate64
#define rvn_pread pread64
#endif


#if defined(__unix__) || defined(__APPLE__)


PRIVATE int32_t /* different impl for linux and mac */
_flush_file(int32_t fd);

PRIVATE int32_t /* different impl for linux and mac */
_sync_directory_allowed(int32_t dir_fd);

PRIVATE int32_t /* different impl for linux and mac */
_finish_open_file_with_odirect(int32_t fd);

PRIVATE int32_t /* different impl for linux and mac */
_rvn_fallocate(int32_t fd, int64_t offset, int64_t size);

PRIVATE char*   /* different impl for linux and mac */
_get_strerror_r(int32_t error, char* tmp_buff, int32_t buf_size);

PRIVATE int64_t
_pwrite(int32_t fd, void *buffer, uint64_t count, uint64_t offset, int32_t *detailed_error_code);

PRIVATE int32_t
_sync_directory_for(const char *file_path, int32_t *detailed_error_code);

PRIVATE int32_t
_sync_directory_for_internal(char *dir_path, int32_t *detailed_error_code);

PRIVATE int32_t
_sync_directory_maybe_symblink(char *dir_path, int32_t depth, int32_t *detailed_error_code);

PRIVATE int32_t
_allocate_file_space(int32_t fd, int64_t size, int32_t *detailed_error_code);

PRIVATE int32_t
_open_file_to_read(const char* file_name, int32_t *fd, int32_t* detailed_error_code);

PRIVATE int32_t
_resize_file(int32_t *fd, int64_t size, int32_t *detailed_error_code);

PRIVATE int32_t
_read_file(int32_t *fd, void* buffer, int64_t required_size, int64_t offset, int64_t* actual_size, int32_t* detailed_error_code);


#endif
#endif
