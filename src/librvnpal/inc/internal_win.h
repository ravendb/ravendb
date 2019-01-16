#ifndef INTERNALWIN_H
#define INTERNALWIN_H

#if defined(_WIN32)

PRIVATE int32_t
_write_file(void* handle, const void* buffer, int64_t size, int64_t offset, int32_t* detailed_error_code);

PRIVATE int32_t
_write_file_incrementally(void* handle, char** buffer, int64_t* size, int64_t* offset, int64_t increment_size, int32_t* detailed_error_code);

#endif
#endif
