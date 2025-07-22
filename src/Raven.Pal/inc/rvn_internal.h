#include "rvn.h"

#define SYS_PAGE_SIZE 4096

extern struct rvn_configuration g_cfg;

PRIVATE
uint64_t nextPowerOf2(uint64_t n);

int32_t
rvn_one_time_init(int32_t *detailed_error_code);

int32_t
rvn_io_ring_init(int32_t* detailed_error_code);

EXPORT int32_t rvn_get_pal_ver();

PRIVATE
int io_ring_setup_successful(void);
