#include <stdlib.h>
#include <errno.h>

#include "rvn.h"
#include "status_codes.h"
#include "slots.h"

PRIVATE int32_t
rvn_slots_init(struct rvn_slots *slots, int32_t capacity, int32_t *detailed_error_code)
{
    *slots = (struct rvn_slots){.capacity = capacity};
    if (capacity <= 0)
    {
        *detailed_error_code = EINVAL;
        return FAIL_INVALID_CONFIGURATION;
    }
    slots->taken = calloc(((size_t)capacity + 63) / 64, sizeof(uint64_t));
    if (slots->taken == NULL)
    {
        *detailed_error_code = ENOMEM;
        return FAIL_NOMEM;
    }
    return SUCCESS;
}

PRIVATE int32_t
rvn_slots_take(struct rvn_slots *slots)
{
    int32_t words = (slots->capacity + 63) / 64;
    for (int32_t word = 0; word < words; word++)
    {
        uint64_t free_bits = ~slots->taken[word];
        if (free_bits == 0)
            continue;
        int32_t bit = __builtin_ctzll(free_bits);
        int32_t slot = word * 64 + bit;
        if (slot >= slots->capacity)
            return -1;
        slots->taken[word] |= 1ULL << bit;
        slots->count++;
        slots->used = rvn_max(slots->used, slot + 1);
        return slot;
    }
    return -1;
}

PRIVATE void
rvn_slots_release(struct rvn_slots *slots, int32_t slot)
{
    slots->taken[slot / 64] &= ~(1ULL << (slot % 64));
    slots->count--;
    if (slot + 1 != slots->used)
        return;
    // find the new highest used slot
    for (int32_t word = slot / 64; word >= 0; word--)
    {
        if (slots->taken[word] != 0)
        {
            slots->used = word * 64 + 64 - __builtin_clzll(slots->taken[word]);
            return;
        }
    }
    slots->used = 0;
}

PRIVATE void
rvn_slots_free(struct rvn_slots *slots)
{
    free(slots->taken);
    *slots = (struct rvn_slots){0};
}
