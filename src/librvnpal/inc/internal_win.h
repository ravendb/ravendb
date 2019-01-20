#ifndef INTERNALWIN_H
#define INTERNALWIN_H

#if defined(_WIN32)

PRIVATE int32_t
_write_file(void* handle, const void* buffer, int64_t size, int64_t offset, int32_t* detailed_error_code);

PRIVATE int32_t
write_file_in_sections(void* handle, const char* buffer, int64_t size, int64_t offset, uint32_t section_size, int32_t* detailed_error_code);

#endif
#endif
