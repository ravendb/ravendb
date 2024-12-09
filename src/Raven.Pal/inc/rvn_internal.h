#include "rvn.h"

#define SYS_PAGE_SIZE               4096

typedef enum rvn_write_mode
{
    rvn_mode_default,
    rvn_write_mode_vectored_file_io,
    rvn_write_mode_file_io,
    rvn_write_mode_io_ring,
    rvn_write_mode_mmap,
} rvn_write_mode;

PRIVATE
rvn_write_mode _get_writer_mode();

PRIVATE
uint64_t nextPowerOf2(uint64_t n);