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

int main()
{
    return write_using_io_ring();
}
