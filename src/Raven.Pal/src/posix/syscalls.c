#if defined(__unix__) && !defined(__APPLE__)

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <unistd.h>
#include <sys/syscall.h>
#include <sys/uio.h>
#include <errno.h>
#include <stdlib.h>

#include "rvn.h"
#include "rvn_internal.h"
#include "internal_posix.h"

/* Raw system calls: glibc's preadv2 needs glibc 2.26 (the PAL needs only 2.17) */

#ifndef RWF_NOWAIT
#define RWF_NOWAIT 0x00000008
#endif

PRIVATE ssize_t
_preadv2_nowait(int fd, void *buffer, size_t length, int64_t offset)
{
    struct iovec iov = { .iov_base = buffer, .iov_len = length };
#if __SIZEOF_LONG__ == 8
    return syscall(SYS_preadv2, fd, &iov, 1, (long)offset, 0L, RWF_NOWAIT);
#else
    return syscall(SYS_preadv2, fd, &iov, 1, (long)(uint32_t)offset, (long)((uint64_t)offset >> 32), RWF_NOWAIT);
#endif
}

/* IORING_MAX_FIXED_FILES of older kernels (5.10); newer kernels allow 1 << 20 */
#define OLD_KERNELS_MAX_FIXED_FILES (1u << 15)

/* Sparse registration needs kernel 5.19; before that, a table of -1 entries */
PRIVATE int
_register_files_empty(struct io_uring *ring, unsigned count)
{
    int rc = io_uring_register_files_sparse(ring, count);
    if (rc != -EINVAL)
        return rc;
    int *files = malloc((size_t)count * sizeof(int));
    if (files == NULL)
        return -ENOMEM;
    for (unsigned i = 0; i < count; i++)
        files[i] = -1;
    rc = io_uring_register_files(ring, files, count);
    free(files);
    return rc;
}

/* Registers an empty file table of up to *count slots, and sets *count to the size the kernel took */
PRIVATE int
_io_uring_register_files_empty(struct io_uring *ring, unsigned *count)
{
    int rc = _register_files_empty(ring, *count);
    if ((rc == -EMFILE || rc == -EINVAL) && *count > OLD_KERNELS_MAX_FIXED_FILES)
    {
        *count = OLD_KERNELS_MAX_FIXED_FILES;
        rc = _register_files_empty(ring, *count);
    }
    return rc;
}

#endif
