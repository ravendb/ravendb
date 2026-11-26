#ifndef RVN_SLOTS_H
#define RVN_SLOTS_H

#include "rvn.h"

/* The slots of a ring's fixed file table; not thread-safe, the owning ring locks it */
struct rvn_slots
{
    int32_t capacity;
    int32_t count;   /* taken slots */
    int32_t used;    /* the highest taken slot + 1 */
    uint64_t *taken; /* a bitmap */
};

PRIVATE int32_t
rvn_slots_init(struct rvn_slots *slots, int32_t capacity, int32_t *detailed_error_code);

/* Returns the lowest free slot, or -1 when every slot is taken */
PRIVATE int32_t
rvn_slots_take(struct rvn_slots *slots);

PRIVATE void
rvn_slots_release(struct rvn_slots *slots, int32_t slot);

PRIVATE void
rvn_slots_free(struct rvn_slots *slots);

#endif
