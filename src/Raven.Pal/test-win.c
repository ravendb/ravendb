#include "rvn.h"
#include <assert.h>
#include "status_codes.h"
#include <stdio.h>
#include <windows.h>
#include "internal_win.h"

int write_using_io_ring()
{
    void* handle = NULL;
    void* mem = NULL;
    void* wmem = NULL;
    int64_t size;
    int32_t err;

    const int32_t pages_count = 34;

    struct rvn_configuration cfg = {
        .io_ring_queue_size = 4,
        .low_priority_io = false,
        .write_mode = rvn_write_mode_io_ring };

    int32_t ec;
    int32_t rc = rvn_startup_configure(&cfg, &ec);
    if (rc != SUCCESS) {
        fprintf(stderr, "rvn_startup_configure failed with status code: %d, error: %d\n", rc, ec);
        return rc;
    }
    printf("rvn_startup_configure - OK\n");

    rc = rvn_init_pager(L"test.db", 1024 * 64, OPEN_FILE_WRITABLE_MAP, &handle, &mem, &wmem, &size, &err);
    if (rc != SUCCESS) {
        fprintf(stderr, "rvn_init_pager failed with status code: %d, error: %d\n", rc, err);
        return rc;
    }
    printf("rvn_init_pager - OK\n");

    char buf[8192] = { 0 };
    buf[1] = 'a';

    for (size_t x = 0; x < 10; x++)
    {
        struct page_to_write* p = calloc(pages_count, sizeof(struct page_to_write));
        if (p == NULL) {
            fprintf(stderr, "calloc failed for page_to_write array\n");
            rvn_close_pager(handle, &err);
            return FAIL_NOMEM;
        }

        for (size_t i = 0; i < pages_count; i++)
        {
            p[i].count_of_pages = 1;
            p[i].ptr = buf;
            p[i].page_num = i;
        }

        rc = rvn_write_io_ring(handle, p, pages_count, &err);
        free(p);

        if (rc != SUCCESS) {
            fprintf(stderr, "rvn_write_io_ring failed with status code: %d, error: %d\n", rc, err);
            rvn_close_pager(handle, &err);
            return rc;
        }
    }

    // Close the pager handle
    rc = rvn_close_pager(handle, &err);
    if (rc != SUCCESS) {
        fprintf(stderr, "rvn_close_pager failed with status code: %d, error: %d\n", rc, err);
        return rc;
    }
    printf("rvn_close_pager - OK\n");

    printf("done\n");
    return SUCCESS;
}

int write_using_mmap(bool do_not_map)
{
    void* handle = NULL;
    void* mem = NULL;
    void* wmem = NULL;
    int64_t size;
    int32_t err;
    const int32_t pages_count = 3;

    struct rvn_configuration cfg = {
        .io_ring_queue_size = 4,
        .low_priority_io = false,
        .write_mode = rvn_write_mode_mmap };

    int32_t ec;
    int32_t rc = rvn_startup_configure(&cfg, &ec);
    if (rc != SUCCESS) {
        fprintf(stderr, "rvn_startup_configure failed with status code: %d, error: %d\n", rc, ec);
        return rc;
    }
    printf("rvn_startup_configure - OK\n");

    int32_t open_flags = OPEN_FILE_NONE;

    if (do_not_map)
    {
        open_flags |= OPEN_FILE_DO_NOT_MAP;
    }

    // Initialize with 2MB (256 pages) to accommodate all tests
    const int64_t initial_size = 256 * VORON_PAGE_SIZE;  // 2MB
    rc = rvn_init_pager(L"test.db", initial_size, open_flags, &handle, &mem, &wmem, &size, &err);
    if (rc != SUCCESS) {
        fprintf(stderr, "rvn_init_pager failed with status code: %d, error: %d\n", rc, err);
        return rc;
    }
    printf("rvn_init_pager - OK (initialized with %lld bytes)\n", initial_size);

    // Get the writer function for mmap mode
    rvn_writer writer = rvn_get_writer(handle);
    if (writer == NULL) {
        fprintf(stderr, "rvn_get_writer returned NULL\n");
        rvn_close_pager(handle, &err);
        return FAIL_INVALID_HANDLE;
    }
    printf("rvn_get_writer - OK\n");

    char* buf = (char*) malloc(256 * VORON_PAGE_SIZE);
    buf[1] = 'a';

    // Test 1: Small writes (original test)
    printf("\n=== Test 1: Small writes (3 pages) ===\n");
    for (size_t x = 0; x < 10; x++)
    {
        struct page_to_write* p = calloc(pages_count, sizeof(struct page_to_write));
        if (p == NULL) {
            fprintf(stderr, "calloc failed for page_to_write array\n");
            rvn_close_pager(handle, &err);
            return FAIL_NOMEM;
        }

        for (size_t i = 0; i < pages_count; i++)
        {
            p[i].count_of_pages = 1;
            p[i].ptr = buf;
            p[i].page_num = i;
        }

        rc = writer(handle, p, pages_count, &err);
        free(p);
        if (rc != SUCCESS) {
            fprintf(stderr, "writer failed with status code: %d, error: %d\n", rc, err);
            rvn_close_pager(handle, &err);
            return rc;
        }
    }
    printf("Test 1 completed successfully\n");

    // Test 2: Write pages that span 64KB boundary (8 pages per write = 64KB)
    printf("\n=== Test 2: Writing 64KB+ (spanning boundary) ===\n");
    const int32_t pages_64kb = 8;  // 8 * 8192 = 64KB
    struct page_to_write* p = calloc(pages_64kb + 2, sizeof(struct page_to_write));
    if (p == NULL) {
        fprintf(stderr, "calloc failed for 64KB test\n");
        rvn_close_pager(handle, &err);
        return FAIL_NOMEM;
    }

    // Write 10 pages starting from page 0 (80KB total, spanning the 64KB boundary)
    for (int32_t i = 0; i < pages_64kb + 2; i++)
    {
        p[i].count_of_pages = 1;
        p[i].ptr = buf;
        p[i].page_num = i;
    }

    rc = writer(handle, p, pages_64kb + 2, &err);
    free(p);
    if (rc != SUCCESS) {
        fprintf(stderr, "writer failed for 64KB+ test with status code: %d, error: %d\n", rc, err);
        rvn_close_pager(handle, &err);
        return rc;
    }
    printf("64KB+ write completed successfully\n");

    // Test 3: Large contiguous write (128KB = 16 pages)
    printf("\n=== Test 3: Large contiguous write (128KB) ===\n");
    const int32_t pages_128kb = 16;
    p = calloc(pages_128kb, sizeof(struct page_to_write));
    if (p == NULL) {
        fprintf(stderr, "calloc failed for 128KB test\n");
        rvn_close_pager(handle, &err);
        return FAIL_NOMEM;
    }

    for (int32_t i = 0; i < pages_128kb; i++)
    {
        p[i].count_of_pages = 1;
        p[i].ptr = buf;
        p[i].page_num = 10 + i;  // Start at page 10 to avoid overlap
    }

    rc = writer(handle, p, pages_128kb, &err);
    free(p);
    if (rc != SUCCESS) {
        fprintf(stderr, "writer failed for 128KB test with status code: %d, error: %d\n", rc, err);
        rvn_close_pager(handle, &err);
        return rc;
    }
    printf("128KB write completed successfully\n");

    // Test 4: Very large write (1MB = 128 pages)
    printf("\n=== Test 4: Very large write (1MB) ===\n");
    const int32_t pages_1mb = 128;
    p = calloc(pages_1mb, sizeof(struct page_to_write));
    if (p == NULL) {
        fprintf(stderr, "calloc failed for 1MB test\n");
        rvn_close_pager(handle, &err);
        return FAIL_NOMEM;
    }

    for (int32_t i = 0; i < pages_1mb; i++)
    {
        p[i].count_of_pages = 1;
        p[i].ptr = buf;
        p[i].page_num = 30 + i;  // Start at page 30
    }

    rc = writer(handle, p, pages_1mb, &err);
    free(p);
    if (rc != SUCCESS) {
        fprintf(stderr, "writer failed for 1MB test with status code: %d, error: %d\n", rc, err);
        rvn_close_pager(handle, &err);
        return rc;
    }
    printf("1MB write completed successfully\n");

    // Test 5: Write with multi-page entries (simulating larger transactions)
    printf("\n=== Test 5: Multi-page entries (spanning 64KB boundaries) ===\n");
    const int32_t multi_page_count = 5;
    p = calloc(multi_page_count, sizeof(struct page_to_write));
    if (p == NULL) {
        fprintf(stderr, "calloc failed for multi-page test\n");
        rvn_close_pager(handle, &err);
        return FAIL_NOMEM;
    }

    // Each entry writes multiple pages (total ~46 pages = 368KB)
    p[0].page_num = 160;
    p[0].count_of_pages = 3;  // 24KB
    p[0].ptr = buf;

    p[1].page_num = 163;
    p[1].count_of_pages = 5;  // 40KB
    p[1].ptr = buf;

    p[2].page_num = 168;
    p[2].count_of_pages = 10; // 80KB (spans 64KB boundary)
    p[2].ptr = buf;

    p[3].page_num = 178;
    p[3].count_of_pages = 8;  // 64KB
    p[3].ptr = buf;

    p[4].page_num = 186;
    p[4].count_of_pages = 20; // 160KB (spans multiple 64KB boundaries)
    p[4].ptr = buf;

    rc = writer(handle, p, multi_page_count, &err);
    free(p);
    if (rc != SUCCESS) {
        fprintf(stderr, "writer failed for multi-page test with status code: %d, error: %d\n", rc, err);
        rvn_close_pager(handle, &err);
        return rc;
    }
    printf("Multi-page entries test completed successfully\n");

    // Test 6: Non-sequential pages across large file range
    printf("\n=== Test 6: Non-sequential pages (testing remapping) ===\n");
    const int32_t non_seq_count = 10;
    p = calloc(non_seq_count, sizeof(struct page_to_write));
    if (p == NULL) {
        fprintf(stderr, "calloc failed for non-sequential test\n");
        rvn_close_pager(handle, &err);
        return FAIL_NOMEM;
    }

    // Write pages at different locations forcing multiple remaps
    // Keep all within the 256-page (2MB) limit
    p[0].page_num = 210;
    p[0].count_of_pages = 1;
    p[0].ptr = buf;

    p[1].page_num = 215;  // Different 64KB chunk
    p[1].count_of_pages = 1;
    p[1].ptr = buf;

    p[2].page_num = 220;  // Another chunk
    p[2].count_of_pages = 1;
    p[2].ptr = buf;

    p[3].page_num = 225;
    p[3].count_of_pages = 1;
    p[3].ptr = buf;

    p[4].page_num = 230;
    p[4].count_of_pages = 5;
    p[4].ptr = buf;

    p[5].page_num = 236;
    p[5].count_of_pages = 5;
    p[5].ptr = buf;

    p[6].page_num = 242;
    p[6].count_of_pages = 1;
    p[6].ptr = buf;

    p[7].page_num = 244;
    p[7].count_of_pages = 1;
    p[7].ptr = buf;

    p[8].page_num = 246;
    p[8].count_of_pages = 5;
    p[8].ptr = buf;

    p[9].page_num = 252;
    p[9].count_of_pages = 3;  // Last pages (up to 254)
    p[9].ptr = buf;

    rc = writer(handle, p, non_seq_count, &err);
    free(p);
    if (rc != SUCCESS) {
        fprintf(stderr, "writer failed for non-sequential test with status code: %d, error: %d\n", rc, err);
        rvn_close_pager(handle, &err);
        return rc;
    }
    printf("Non-sequential pages test completed successfully\n");

    // Close the pager handle
    rc = rvn_close_pager(handle, &err);
    if (rc != SUCCESS) {
        fprintf(stderr, "rvn_close_pager failed with status code: %d, error: %d\n", rc, err);
        return rc;
    }
    printf("rvn_close_pager - OK\n");

    printf("\n=== All tests completed successfully ===\n");
    return SUCCESS;
}

int main()
{
    return write_using_mmap(true);
}
