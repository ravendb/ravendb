/* 
    Dirty-range tracking + paced writeback for pagers. Allows us to better control how we
    write dirty pages to disk, to avoid IO avalances when we call fdatasync after a lot of work.
    Doing it in this manners smooth out the background work for flush & sync of the data, avoiding
    parking GBs of writes in front of the journal writes (thus spiking the transaction latencies).

    This is an _optimization_, not correctness issue. We stil rely on fdatasync for durability, but
    we want to ensure that it has little to do by the time we call it.

    This uses a bitmap with one bit per page (set when a page is dirtied) to track the dirty ranges.
    Rely on Voron's single writer for concurrency, with a known race when reading (during flush / sync).
    This is acceptable because the bitmap for pacing the I/O, not for durability. That remains on fdatasync.
*/


#define WRITEBACK_BYTES_PER_BIT VORON_PAGE_SIZE
#define WRITEBACK_DEFAULT_BLOCK_SIZE (32 * 1024 * 1024)
#define WRITEBACK_MAX_PIPELINE_DEPTH 16

#if defined(_MSC_VER)
#include <intrin.h>
#define rvn_atomic_xchg64(p, v) InterlockedExchange64((volatile LONG64 *)(p), (LONG64)(v))
#define rvn_atomic_or64(p, v) InterlockedOr64((volatile LONG64 *)(p), (LONG64)(v))
#define rvn_atomic_load_ptr(p) InterlockedCompareExchangePointer((PVOID volatile *)(p), NULL, NULL)
#define rvn_atomic_store_ptr(p, v) InterlockedExchangePointer((PVOID volatile *)(p), (v))
static int32_t rvn_ctz64(uint64_t v)
{
    if(!v) return 64;
    unsigned long i;
    _BitScanForward64(&i, v);
    return (int32_t)i;
}
#define rvn_popcnt64(v) ((int32_t)__popcnt64(v))
#else
#define rvn_atomic_xchg64(p, v) __atomic_exchange_n((p), (v), __ATOMIC_ACQ_REL)
#define rvn_atomic_or64(p, v) __atomic_fetch_or((p), (v), __ATOMIC_ACQ_REL)
#define rvn_atomic_load_ptr(p) __atomic_load_n((p), __ATOMIC_ACQUIRE)
#define rvn_atomic_store_ptr(p, v) __atomic_store_n((p), (v), __ATOMIC_RELEASE)
#define rvn_ctz64(v) (v ? (int32_t)__builtin_ctzll(v) : 64)
#define rvn_popcnt64(v) ((int32_t)__builtin_popcountll(v))
#endif

#define WRITEBACK_MIN_BITMAP_WORDS 128 /* 1KB of bitmap = 64MB of file */

struct dirty_bitmap
{
    int64_t number_of_words;
    struct dirty_bitmap *prev; /* retired generations, freed with global_state */
    uint64_t words[];
};

PRIVATE int32_t
_writeback_supported(struct handle *handle_ptr);

PRIVATE int32_t
_writeback_range_start(struct handle *handle_ptr, int64_t offset, int64_t length, int32_t *detailed_error_code);

PRIVATE int32_t
_writeback_range_complete(struct handle *handle_ptr, int64_t offset, int64_t length, int32_t *detailed_error_code);

static bool
_should_track_dirty_ranges(int32_t open_flags)
{
    return (open_flags & (OPEN_FILE_TEMPORARY | OPEN_FILE_READ_ONLY | OPEN_FILE_COPY_ON_WRITE)) == 0;
}

PRIVATE void
_free_dirty_bitmaps(struct dirty_bitmap *bm)
{
    while (bm != NULL)
    {
        struct dirty_bitmap *prev = bm->prev;
        free(bm);
        bm = prev;
    }
}

static struct dirty_bitmap *
_grow_dirty_bitmap(struct handle_global_state *global_state, struct dirty_bitmap *current, int64_t max_bit)
{
    int64_t words = current != NULL ? current->number_of_words * 2 : WRITEBACK_MIN_BITMAP_WORDS;
    while (words * 64 <= max_bit)
        words *= 2;

    // no concurrency to worry about, we are called under the lock

    struct dirty_bitmap *bm = calloc(1, sizeof(struct dirty_bitmap) + (size_t)words * sizeof(uint64_t));
    if (bm == NULL)
        return current; /* tracking degrades, fdatasync still covers everything */

    bm->number_of_words = words;
    bm->prev = current; // we will free the previous generations when the file is closed (cheap, safe to concurrent readers)
    if (current != NULL)
    {
        /* safe to race against a concurrent drain clearing words - we'll just re-clear it next time */
        memcpy(bm->words, (void *)current->words, (size_t)current->number_of_words * sizeof(uint64_t));
    }
    rvn_atomic_store_ptr(&global_state->dirty_bitmap, bm);
    return bm;
}

static void
_set_dirty_bits(struct dirty_bitmap *bm, int64_t start_bit, int64_t bit_count)
{
    for (int64_t bit = start_bit; bit < start_bit + bit_count;)
    {
        int64_t word = bit >> 6;
        int32_t bit_in_word = (int32_t)(bit & 63);
        int64_t bits_in_this_word = rvn_min(64 - bit_in_word, start_bit + bit_count - bit);
        uint64_t mask = bits_in_this_word == 64
                            ? ~0ULL
                            : (((1ULL << bits_in_this_word) - 1) << bit_in_word);
        rvn_atomic_or64(&bm->words[word], mask);
        bit += bits_in_this_word;
    }
}

PRIVATE void
_mark_dirty_pages(void *handle, struct page_to_write *buffers, int32_t count)
{
    struct handle *handle_ptr = handle;
    struct handle_global_state *global_state = handle_ptr->global_state;
    if (count <= 0 || !_should_track_dirty_ranges(global_state->open_flags))
        return;

    // buffers is sorted, the last buffer has the highest page number
    int64_t max_bit = ((buffers[count - 1].page_num + buffers[count - 1].count_of_pages) * VORON_PAGE_SIZE - 1) / WRITEBACK_BYTES_PER_BIT;

    struct dirty_bitmap *bm = rvn_atomic_load_ptr(&global_state->dirty_bitmap);
    if (bm == NULL || max_bit >= bm->number_of_words * 64)
        bm = _grow_dirty_bitmap(global_state, bm, max_bit);
    if (bm == NULL || max_bit >= bm->number_of_words * 64)
        return; /* allocation failed, fallback to fdatasync */

    for (int32_t i = 0; i < count; i++)
    {
        int64_t first_bit = (buffers[i].page_num * VORON_PAGE_SIZE) / WRITEBACK_BYTES_PER_BIT;
        int64_t last_bit = ((buffers[i].page_num + buffers[i].count_of_pages) * VORON_PAGE_SIZE - 1) / WRITEBACK_BYTES_PER_BIT;
        _set_dirty_bits(bm, first_bit, last_bit - first_bit + 1);
    }
}

struct writeback_ctx
{
    struct handle *handle;
    struct rvn_writeback_stats *stats;
    int32_t depth;
    bool initiate_only; /* trickle mode: start writeback, don't wait on it */
    int32_t pending_count;
    int32_t pending_head;
    struct
    {
        int64_t offset;
        int64_t length;
        int64_t busy_ticks; /* time already spent in the start call */
    } pending[WRITEBACK_MAX_PIPELINE_DEPTH];
};

static int32_t
_writeback_complete_oldest(struct writeback_ctx *ctx, int32_t *detailed_error_code)
{
    int32_t slot = ctx->pending_head;
    int64_t before = _stopwatch();
    int32_t rc = _writeback_range_complete(ctx->handle, ctx->pending[slot].offset, ctx->pending[slot].length, detailed_error_code);
    int64_t range_ticks = _elapsed(before, _stopwatch()) + ctx->pending[slot].busy_ticks;

    ctx->stats->total_wait_ticks += range_ticks;
    if (range_ticks > ctx->stats->max_range_wait_ticks)
        ctx->stats->max_range_wait_ticks = range_ticks;

    ctx->pending_head = (ctx->pending_head + 1) % WRITEBACK_MAX_PIPELINE_DEPTH;
    ctx->pending_count--;
    return rc;
}

static int32_t
_writeback_emit(struct writeback_ctx *ctx, int64_t offset, int64_t length, int32_t *detailed_error_code)
{
    if (!ctx->initiate_only && ctx->pending_count == ctx->depth)
    {
        int32_t rc = _writeback_complete_oldest(ctx, detailed_error_code);
        if (rc != SUCCESS)
            return rc;
    }

    int64_t before = _stopwatch();
    int32_t rc = _writeback_range_start(ctx->handle, offset, length, detailed_error_code);
    if (rc != SUCCESS)
        return rc;

    if (!ctx->initiate_only)
    {
        int32_t slot = (ctx->pending_head + ctx->pending_count) % WRITEBACK_MAX_PIPELINE_DEPTH;
        ctx->pending[slot].offset = offset;
        ctx->pending[slot].length = length;
        ctx->pending[slot].busy_ticks = _elapsed(before, _stopwatch());
        ctx->pending_count++;
    }

    ctx->stats->bytes_written += length;
    ctx->stats->ranges_written++;
    return SUCCESS;
}

static int32_t
_writeback_emit_run(struct writeback_ctx *ctx, int64_t start_bit, int64_t bit_count,
             int64_t block_bits, int32_t *detailed_error_code)
{
    while (bit_count > 0)
    {
        int64_t bits = rvn_min(bit_count, block_bits);
        int32_t rc = _writeback_emit(ctx, start_bit * WRITEBACK_BYTES_PER_BIT, bits * WRITEBACK_BYTES_PER_BIT, detailed_error_code);
        if (rc != SUCCESS)
            return rc;
        start_bit += bits;
        bit_count -= bits;
    }
    return SUCCESS;
}

static int32_t
_writeback_finish_run(struct writeback_ctx *ctx, int64_t start_bit, int64_t bit_count,
                      int64_t block_bits, int64_t min_bits, bool partially_emitted,
                      int32_t *detailed_error_code)
{
    if (partially_emitted == false && bit_count < min_bits)
    {
        ctx->stats->bytes_skipped += bit_count * WRITEBACK_BYTES_PER_BIT;
        return SUCCESS;
    }
    return _writeback_emit_run(ctx, start_bit, bit_count, block_bits, detailed_error_code);
}

EXPORT int32_t
rvn_pager_writeback_dirty(void *handle,
                          int32_t pipeline_depth,
                          int32_t block_size_bytes,
                          int32_t min_write_size_bytes,
                          struct rvn_writeback_stats *stats,
                          int32_t *detailed_error_code)
{
    struct handle *handle_ptr = handle;
    struct handle_global_state *global_state = handle_ptr->global_state;
    memset(stats, 0, sizeof(*stats));
    *detailed_error_code = 0;

    struct dirty_bitmap *bm = rvn_atomic_load_ptr(&global_state->dirty_bitmap);
    if (bm == NULL)
        return SUCCESS; /* nothing was ever tracked for this pager */

    if (!_writeback_supported(handle_ptr))
        return FAIL_WRITEBACK_NOT_SUPPORTED;

    /* depth 0 = trickle: initiate writeback and return without ever waiting on it */
    bool initiate_only = pipeline_depth == 0;
    if (pipeline_depth < 1)
        pipeline_depth = 1;
    if (pipeline_depth > WRITEBACK_MAX_PIPELINE_DEPTH)
        pipeline_depth = WRITEBACK_MAX_PIPELINE_DEPTH;
    if (block_size_bytes < WRITEBACK_BYTES_PER_BIT)
        block_size_bytes = WRITEBACK_BYTES_PER_BIT;
    int64_t block_bits = block_size_bytes / WRITEBACK_BYTES_PER_BIT;
    int64_t min_bits = min_write_size_bytes > 0 ? min_write_size_bytes / WRITEBACK_BYTES_PER_BIT : 0;

    struct writeback_ctx ctx = {
        .handle = handle_ptr,
        .stats = stats,
        .depth = pipeline_depth,
        .initiate_only = initiate_only,
    };

    int32_t rc = SUCCESS;
    int64_t run_start_bit = -1;
    int64_t run_bits = 0;
    bool run_emitted = false;

    for (int64_t w = 0; w < bm->number_of_words; w++)
    {
        uint64_t bits = bm->words[w]; // checking without atomics here, mostly they are 0, so this is cheap
        if (bits != 0)
        {
            // atomically clear the word, then do the I/O, in the meantime writes may set more bits, we'll get them next time
            bits = rvn_atomic_xchg64(&bm->words[w], 0);
        }

        if (bits == 0)
        {
            if (run_start_bit >= 0)
            {
                rc = _writeback_finish_run(&ctx, run_start_bit, run_bits, block_bits, min_bits, run_emitted, detailed_error_code);
                run_start_bit = -1;
                run_bits = 0;
                run_emitted = false;
                if (rc != SUCCESS)
                    goto done;
            }
            continue;
        }

        int64_t word_base = w * 64;

        while (bits != 0)
        {
            int32_t first = rvn_ctz64(bits);
            uint64_t shifted = bits >> first;
            int32_t segment = rvn_ctz64(~shifted);
            int64_t segment_start = word_base + first;

            if (run_start_bit >= 0 && segment_start != run_start_bit + run_bits)
            {
                rc = _writeback_finish_run(&ctx, run_start_bit, run_bits, block_bits, min_bits, run_emitted, detailed_error_code);
                run_start_bit = -1;
                run_bits = 0;
                run_emitted = false;
                if (rc != SUCCESS)
                    goto done;
            }
            if (run_start_bit < 0)
                run_start_bit = segment_start;
            run_bits += segment;

            /* emit whole blocks eagerly so the pipeline fills as we scan */
            while (run_bits >= block_bits)
            {
                rc = _writeback_emit(&ctx, run_start_bit * WRITEBACK_BYTES_PER_BIT, block_bits * WRITEBACK_BYTES_PER_BIT, detailed_error_code);
                if (rc != SUCCESS)
                    goto done;
                run_start_bit += block_bits;
                run_bits -= block_bits;
                run_emitted = true;
            }

            if (first + segment >= 64)
                bits = 0;
            else
                bits &= ~(((1ULL << segment) - 1) << first);
        }
    }

    if (run_start_bit >= 0)
    {
        rc = _writeback_finish_run(&ctx, run_start_bit, run_bits, block_bits, min_bits, run_emitted, detailed_error_code);
        if (rc != SUCCESS)
            goto done;
    }

done:
    while (ctx.pending_count > 0)
    {
        int32_t completion_error = 0;
        int32_t wait_rc = _writeback_complete_oldest(&ctx, rc == SUCCESS ? detailed_error_code : &completion_error);
        if (wait_rc != SUCCESS && rc == SUCCESS)
            rc = wait_rc;
    }

    /* racy diagnostic count - plain loads, let it vectorize */
    for (int64_t w = 0; w < bm->number_of_words; w++)
    {
        stats->set_bits_remaining += rvn_popcnt64(bm->words[w]);
    }
    return rc;
}
