#include <rvn.h>
#include <string.h>
#include <stdint.h>


int rvn_memcmp(const void *s1, const void *s2, int32_t n)
{
	return memcmp(s1, s2, n);
}
