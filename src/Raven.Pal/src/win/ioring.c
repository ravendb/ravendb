// required to enable io ring support
#define NTDDI_VERSION NTDDI_WIN10_NI

#include <windows.h>
#include <versionhelpers.h>
#include <stdio.h>
#include <ioringapi.h>

#include "rvn.h"
#include "rvn_internal.h"
#include "status_codes.h"
#include "internal_win.h"
#include "slots.h"

// Define missing constant for zig's mingw headers
#ifndef IORING_E_SUBMISSION_QUEUE_FULL
#define IORING_E_SUBMISSION_QUEUE_FULL ((HRESULT)0x80460002L)
#endif

typedef HRESULT(WINAPI *PFN_QueryIoRingCapabilities)(IORING_CAPABILITIES *);
typedef HRESULT(WINAPI *PFN_CloseIoRing)(HIORING);
typedef HRESULT(WINAPI *PFN_CreateIoRing)(IORING_VERSION, IORING_CREATE_FLAGS, UINT32, UINT32, HIORING *);
typedef HRESULT(WINAPI *PFN_SubmitIoRing)(HIORING, UINT32, UINT32, UINT32 *);
typedef HRESULT(WINAPI *PFN_PopIoRingCompletion)(HIORING, IORING_CQE *);
typedef HRESULT(WINAPI *PFN_BuildIoRingWriteFile)(HIORING, IORING_HANDLE_REF, IORING_BUFFER_REF, UINT32, UINT64, FILE_WRITE_FLAGS, UINT_PTR, IORING_SQE_FLAGS);
typedef HRESULT(WINAPI *PFN_SetIoRingCompletionEvent)(HIORING, HANDLE);
typedef HRESULT(WINAPI *PFN_BuildIoRingRegisterFileHandles)(HIORING, UINT32, HANDLE const[], UINT_PTR);

typedef enum workitem_type
{
    workitem_none,
    workitem_write,
    workitem_fsync,
    workitem_register_files,
} workitem_type;

struct submittion
{
    int32_t file; /* a slot in the ring's file table */
    HANDLE notify;
    int32_t count;
    int32_t result;
    bool error;
};

struct workitem
{
    struct submittion *submittion;
    struct workitem *next;
    char *buffer;

    uint64_t offset;
    uint64_t size;

    /* workitem_register_files: the whole table, a registration replaces the previous one */
    HANDLE *files;
    UINT32 files_count;

    workitem_type type;
};

struct IoRingSetup
{
    PFN_QueryIoRingCapabilities QueryIoRingCapabilities;
    PFN_CloseIoRing CloseIoRing;
    PFN_CreateIoRing CreateIoRing;
    PFN_SubmitIoRing SubmitIoRing;
    PFN_PopIoRingCompletion PopIoRingCompletion;
    PFN_BuildIoRingWriteFile BuildIoRingWriteFile;
    PFN_SetIoRingCompletionEvent SetIoRingCompletionEvent;
    PFN_BuildIoRingRegisterFileHandles BuildIoRingRegisterFileHandles;

    HIORING io_ring;
    HANDLE event;
    HANDLE thread;
    HANDLE has_error;
    long errored;
    struct workitem *volatile head;

    // guards slots, files and files_limit, and keeps files unchanged until the ring completes their registration
    SRWLOCK files_lock;
    struct rvn_slots slots;
    HANDLE *files;         /* the ring's duplicate of each registered file; a free slot holds empty_file */
    int32_t files_length;  /* grows as slots are taken, up to slots.capacity */
    HANDLE empty_file;     /* the NUL device, read-only: a write to a free slot fails */
    int32_t files_limit;   /* tests: registrations fail once this many files are held, -1 for none */
};

struct IoRingSetup IoRing;

static int32_t hresult_to_detailed_error(HRESULT hr)
{
    // We need to translate HRESULT codes into Win32 error codes to have correct handling inside RavenDB.
    // If we cannot convert to Win32, return the original value.
    if (HRESULT_FACILITY(hr) == FACILITY_WIN32)
    {
        return HRESULT_CODE(hr);
    }

    return hr;
}

bool FillIoRingFunctions(struct IoRingSetup *s)
{
    memset(s, 0, sizeof(struct IoRingSetup));
    HMODULE hKernelDll = LoadLibrary(TEXT("kernel32.dll"));
    if (hKernelDll == NULL)
    {
        return true;
    }

    s->QueryIoRingCapabilities = (PFN_QueryIoRingCapabilities)GetProcAddress(hKernelDll, "QueryIoRingCapabilities");
    s->CloseIoRing = (PFN_CloseIoRing)GetProcAddress(hKernelDll, "CloseIoRing");
    s->CreateIoRing = (PFN_CreateIoRing)GetProcAddress(hKernelDll, "CreateIoRing");
    s->SubmitIoRing = (PFN_SubmitIoRing)GetProcAddress(hKernelDll, "SubmitIoRing");
    s->PopIoRingCompletion = (PFN_PopIoRingCompletion)GetProcAddress(hKernelDll, "PopIoRingCompletion");
    s->BuildIoRingWriteFile = (PFN_BuildIoRingWriteFile)GetProcAddress(hKernelDll, "BuildIoRingWriteFile");
    s->SetIoRingCompletionEvent = (PFN_SetIoRingCompletionEvent)GetProcAddress(hKernelDll, "SetIoRingCompletionEvent");
    s->BuildIoRingRegisterFileHandles = (PFN_BuildIoRingRegisterFileHandles)GetProcAddress(hKernelDll, "BuildIoRingRegisterFileHandles");

    FreeLibrary(hKernelDll);

    return (s->QueryIoRingCapabilities &&
            s->CloseIoRing &&
            s->CreateIoRing &&
            s->SubmitIoRing &&
            s->PopIoRingCompletion &&
            s->SetIoRingCompletionEvent &&
            s->BuildIoRingWriteFile &&
            s->BuildIoRingRegisterFileHandles);
}

void queue_work(struct workitem *head, struct workitem *last)
{
    while (true)
    {
        struct workitem *cur_head = IoRing.head;
        last->next = cur_head;
        if (InterlockedCompareExchangePointer((void *volatile *)&IoRing.head, head, cur_head) == cur_head)
            break;
    }
}

void close_ring_with_error(HRESULT hr)
{
    UINT32 events_submitted = 0;
    // read the number of events submitted
    IoRing.SubmitIoRing(IoRing.io_ring, 0, 0, &events_submitted);
    // wait for all the submitted events to complete
    IoRing.SubmitIoRing(IoRing.io_ring, events_submitted, INFINITE, NULL);
    IORING_CQE cqe;
    while (SUCCEEDED(IoRing.PopIoRingCompletion(IoRing.io_ring, &cqe)))
    {
        struct workitem *work = (struct workitem *)cqe.UserData;
        work->submittion->result = hresult_to_detailed_error(hr);        
        work->submittion->error = true;
        if (--work->submittion->count == 0)
        {
            SetEvent(work->submittion->notify);
        }
    }
    IoRing.CloseIoRing(IoRing.io_ring);
    IoRing.io_ring = NULL;
}

DWORD WINAPI do_ring_work(LPVOID lpThreadParameter)
{
    // Set I/O priority to a low value (Best Effort class with lowest priority within class)
    // since we want to use this thread for running flush & sync operation in the background
    // and those are not urgent. We'll let the OS schedule them as needed.
    if (g_cfg.low_priority_io)
    {
        DWORD result = SetThreadPriority(GetCurrentThread(), THREAD_MODE_BACKGROUND_BEGIN);
        (void)result; // explicitly ignoring this error, it doesn't matter if we cannot do that
    }
    SetThreadDescription(GetCurrentThread(), L"Rvn.Ring.Wrkr");
    HIORING ring = IoRing.io_ring;
    struct workitem *work = NULL;
    HRESULT hr = 0;
    while (true)
    {
        // wait for any writes on the event / completion on the ring
        if (WaitForSingleObject(IoRing.event, INFINITE) != WAIT_OBJECT_0)
        {
            hr = GetLastError();
            goto error;
        }
        ResetEvent(IoRing.event);
        bool has_work = true;
        while (has_work)
        {
            has_work = false;
            bool must_wait = false;
            if (!work) // we may have _previous_ work to run through
            {
                work = InterlockedExchangePointer((void *volatile *)&IoRing.head, 0);
            }
            while (work)
            {
                has_work = true;
                if (work->type == workitem_register_files)
                {
                    hr = IoRing.BuildIoRingRegisterFileHandles(ring, work->files_count, work->files, (UINT_PTR)work);
                }
                else
                {
                    IORING_HANDLE_REF file_handle_ref = IoRingHandleRefFromIndex((UINT32)work->submittion->file);
                    IORING_BUFFER_REF buffer_ref = IoRingBufferRefFromPointer(work->buffer);
                    int32_t size_to_write = (int32_t)(rvn_min(work->size, INT32_MAX));
                    hr = IoRing.BuildIoRingWriteFile(ring, file_handle_ref,
                                                     buffer_ref, size_to_write, work->offset, FILE_WRITE_FLAGS_NONE,
                                                     (UINT_PTR)work, IOSQE_FLAGS_NONE);
                }

                if (hr == IORING_E_SUBMISSION_QUEUE_FULL)
                {
                    must_wait = true; // need to submit the work we have so far, and retry later
                    break;
                }
                if (FAILED(hr))
                    goto error;

                work = work->next;
            }
            hr = must_wait ? IoRing.SubmitIoRing(ring, 1, INFINITE, NULL) : IoRing.SubmitIoRing(ring, 0, 0, NULL);
            if (FAILED(hr))
                goto error;

            IORING_CQE cqe;
            while (IoRing.PopIoRingCompletion(ring, &cqe) == S_OK)
            {
                has_work = true;
                struct workitem *cur = (struct workitem *)cqe.UserData;
                switch (cur->type)
                {
                case workitem_write:
                    if (SUCCEEDED(cqe.ResultCode))
                    {
                        cur->offset += (uint64_t)cqe.Information;
                        cur->buffer += cqe.Information;
                        cur->size -= (uint64_t)cqe.Information;
                        if (cur->size)
                        {
                            queue_work(cur, cur);
                            continue;
                        }
                    }
                    else
                    {
                        cur->submittion->error = true;
                        cur->submittion->result = hresult_to_detailed_error(cqe.ResultCode);
                    }
                    break;

                default:
                    if (FAILED(cqe.ResultCode))
                    {
                        cur->submittion->error = true;
                        cur->submittion->result = hresult_to_detailed_error(cqe.ResultCode);
                    }
                    break;
                }
                if (--cur->submittion->count == 0)
                {
                    SetEvent(cur->submittion->notify);
                }
            }
        }
    }
error:
    InterlockedExchange(&IoRing.errored, hr);
    SetEvent(IoRing.has_error);
    close_ring_with_error(hr);
    IoRing.io_ring = NULL;
    // here we are going to keep answering all future inquires
    // with an immediate error
    while (true)
    {
        WaitForSingleObject(IoRing.event, INFINITE);
        ResetEvent(IoRing.event);
        work = InterlockedExchangePointer((void *volatile *)&IoRing.head, 0);
        while (work)
        {
            struct workitem *n = work->next;

            work->submittion->result = ERROR_IO_INCOMPLETE;
            work->submittion->error = true;
            if (--work->submittion->count == 0)
            {
                SetEvent(work->submittion->notify);
            }
            work = n;
        }
    }
    return 0;
}

int32_t rvn_write_io_ring(
    void *handle,
    struct page_to_write *buffers,
    int32_t count,
    int32_t *detailed_error_code)
{
    int32_t rc = SUCCESS;
    struct handle *handle_ptr = handle;
    if (count == 0)
        return SUCCESS;

    if (WaitForSingleObject(IoRing.has_error, 0) == WAIT_OBJECT_0)
    {
        *detailed_error_code = ERROR_IO_INCOMPLETE;
        return FAIL_IO_RING_SUBMIT;
    }

    if (handle_ptr->global_state->io_ring_file == -1)
    {
        *detailed_error_code = ERROR_INVALID_HANDLE; // a read-only file, or one written through a writable map
        return FAIL_IO_RING_WRITE;
    }

    EnterCriticalSection(&handle_ptr->global_state->lock);

    size_t max_req_size = (size_t)count * sizeof(struct workitem);
    if (handle_ptr->global_state->arena_size < max_req_size)
    {
        size_t size = (size_t)nextPowerOf2(max_req_size);
        void *ptr = realloc(handle_ptr->global_state->arena, size);
        if (!ptr)
        {
            *detailed_error_code = errno;
            rc = FAIL_NOMEM;
            goto exit;
        }
        handle_ptr->global_state->arena = ptr;
        handle_ptr->global_state->arena_size = size;
    }

    struct submittion submittion = {
        .file = handle_ptr->global_state->io_ring_file,
        .notify = handle_ptr->global_state->notify,
        .count = count,
    };

    char *buf = handle_ptr->global_state->arena;
    struct workitem *head = NULL;
    for (int32_t curIdx = count - 1; curIdx >= 0; curIdx--)
    {
        uint64_t offset = buffers[curIdx].page_num * VORON_PAGE_SIZE;
        uint64_t size = (uint64_t)buffers[curIdx].count_of_pages * VORON_PAGE_SIZE;

        struct workitem *work = (struct workitem *)buf;
        *work = (struct workitem){
            .buffer = buffers[curIdx].ptr,
            .size = size,
            .type = workitem_write,
            .offset = offset,
            .next = head,
            .submittion = &submittion,
        };
        head = work;
        buf += sizeof(struct workitem);
    }

    rc = SUCCESS;
    ResetEvent(handle_ptr->global_state->notify);
    struct workitem *last = handle_ptr->global_state->arena;
    queue_work(head, last);
    SetEvent(IoRing.event);
    if (WaitForSingleObject(handle_ptr->global_state->notify, INFINITE) == WAIT_FAILED)
    {
        *detailed_error_code = GetLastError();
        rc = FAIL_POLL_EVENTFD;
        goto exit;
    }

    if (submittion.error)
    {
        *detailed_error_code = submittion.result;
        rc = FAIL_IO_RING_WRITE;
    }
    else
    {
        _mark_dirty_pages(handle, buffers, count);
    }

exit:
    LeaveCriticalSection(&handle_ptr->global_state->lock);
    return rc;
}

PRIVATE int32_t
rvn_one_time_init(int32_t* detailed_error_code)
{
    switch(g_cfg.write_mode)
    {
        case rvn_write_mode_io_ring:
            return rvn_io_ring_init(detailed_error_code);
       
        case rvn_mode_default:
            if (rvn_io_ring_init(detailed_error_code) == SUCCESS)
            {
                g_cfg.write_mode = rvn_write_mode_io_ring;
                return SUCCESS;
            }
            // no break here, we want to fall through
        case rvn_write_mode_file_io:
            g_cfg.write_mode = rvn_write_mode_file_io;
            return SUCCESS;
        default:
            *detailed_error_code = ERROR_NOT_SUPPORTED;
            return FAIL_INVALID_CONFIGURATION;
    }
}

PRIVATE int32_t
rvn_io_ring_init(int32_t *detailed_error_code)
{
    if (g_cfg.io_ring_queue_size < 0 || !FillIoRingFunctions(&IoRing)) 
    {
        *detailed_error_code = ERROR_NOT_SUPPORTED;
        return FAIL_CREATE_IO_RING; // not supported  or disabled, that is fine...
    }
    
    int rc = rvn_slots_init(&IoRing.slots, g_cfg.io_ring_max_files, detailed_error_code);
    if (rc != SUCCESS)
        goto error;
    IoRing.empty_file = CreateFileW(L"NUL", GENERIC_READ, FILE_SHARE_READ | FILE_SHARE_WRITE, NULL, OPEN_EXISTING, 0, NULL);
    if (IoRing.empty_file == INVALID_HANDLE_VALUE)
    {
        IoRing.empty_file = NULL;
        *detailed_error_code = GetLastError();
        rc = FAIL_OPEN_FILE;
        goto error;
    }
    IoRing.files_limit = -1;
    InitializeSRWLock(&IoRing.files_lock);

    IORING_CREATE_FLAGS flags = {0};
    HRESULT hr = IoRing.CreateIoRing(IORING_VERSION_3, flags, g_cfg.io_ring_queue_size, g_cfg.io_ring_queue_size * 2, &IoRing.io_ring);
    if (FAILED(hr))
    {
        *detailed_error_code = hresult_to_detailed_error(hr);
        rc = FAIL_CREATE_IO_RING;
        goto error;
    }
    IoRing.event = CreateEvent(NULL, TRUE, FALSE, NULL);
    if (IoRing.event == NULL)
    {
        *detailed_error_code = GetLastError();
        rc = FAIL_CREATE_EVENTFD;
        goto error;
    }
    IoRing.has_error = CreateEvent(NULL, TRUE, FALSE, NULL);
    if (IoRing.has_error == NULL)
    {
        *detailed_error_code = GetLastError();
        rc = FAIL_CREATE_EVENTFD;
        goto error;
    }
    hr = IoRing.SetIoRingCompletionEvent(IoRing.io_ring, IoRing.event);
    if (FAILED(hr))
    {
        *detailed_error_code = hresult_to_detailed_error(hr);
        rc = FAIL_IO_RING_REG_EVENTFD;
        goto error;
    }
    IoRing.thread = CreateThread(NULL, 0, do_ring_work, NULL, 0, NULL);
    if (IoRing.thread == NULL)
    {
        *detailed_error_code = GetLastError();
        rc = FAIL_CREATE_THREAD;
        goto error;
    }
    return SUCCESS;
error:
    if (IoRing.io_ring)
        IoRing.CloseIoRing(IoRing.io_ring);
    IoRing.io_ring = NULL;
    if (IoRing.event != NULL)
        CloseHandle(IoRing.event);
    if (IoRing.has_error != NULL)
        CloseHandle(IoRing.has_error);
    IoRing.event = NULL;
    IoRing.has_error = NULL;
    IoRing.thread = NULL;
    if (IoRing.empty_file != NULL)
        CloseHandle(IoRing.empty_file);
    IoRing.empty_file = NULL;
    free(IoRing.files);
    IoRing.files = NULL;
    rvn_slots_free(&IoRing.slots);
    return rc;
}

int io_ring_setup_successful()
{
    return IoRing.io_ring != NULL;
}

/* Registers files[0, used) through the worker, the only thread that builds requests. Called under files_lock. */
static int32_t
_register_files(HANDLE notify, int32_t *detailed_error_code)
{
    struct submittion submittion = {
        .file = -1,
        .notify = notify,
        .count = 1,
    };
    struct workitem work = {
        .submittion = &submittion,
        .type = workitem_register_files,
        .files = IoRing.files,
        .files_count = (UINT32)rvn_max(IoRing.slots.used, 1), // with no file, registers the empty file
    };
    ResetEvent(notify);
    queue_work(&work, &work);
    SetEvent(IoRing.event);
    if (WaitForSingleObject(notify, INFINITE) == WAIT_FAILED)
    {
        *detailed_error_code = GetLastError();
        return FAIL_POLL_EVENTFD;
    }
    if (submittion.error)
    {
        *detailed_error_code = submittion.result;
        return FAIL_IO_RING_REGISTER_FILE;
    }
    return SUCCESS;
}

static bool
_grow_files(int32_t slot, int32_t *detailed_error_code)
{
    int32_t length = rvn_max(IoRing.files_length * 2, 64);
    while (length <= slot)
        length *= 2;
    length = rvn_min(length, IoRing.slots.capacity);
    HANDLE *grown = realloc(IoRing.files, (size_t)length * sizeof(HANDLE));
    if (grown == NULL)
    {
        *detailed_error_code = ERROR_NOT_ENOUGH_MEMORY;
        return false;
    }
    for (int32_t i = IoRing.files_length; i < length; i++)
        grown[i] = IoRing.empty_file;
    IoRing.files = grown;
    IoRing.files_length = length;
    return true;
}

EXPORT int32_t
rvn_io_ring_limit_files_for_testing(int32_t max)
{
    AcquireSRWLockExclusive(&IoRing.files_lock);
    IoRing.files_limit = max < 0 ? -1 : IoRing.slots.count + max;
    ReleaseSRWLockExclusive(&IoRing.files_lock);
    return SUCCESS;
}

/* The ring registers its own duplicate of the handle, so the slot outlives the pager's handles */
PRIVATE int32_t
rvn_io_ring_register_file(struct handle_global_state *global_state, HANDLE file, int32_t *detailed_error_code)
{
    HANDLE registered;
    if (!DuplicateHandle(GetCurrentProcess(), file, GetCurrentProcess(), &registered, 0, FALSE, DUPLICATE_SAME_ACCESS))
    {
        *detailed_error_code = GetLastError();
        return FAIL_DUPLICATE_HANDLE;
    }
    int32_t rc = SUCCESS;
    AcquireSRWLockExclusive(&IoRing.files_lock);
    int32_t slot = IoRing.files_limit != -1 && IoRing.slots.count >= IoRing.files_limit ? -1 : rvn_slots_take(&IoRing.slots);
    if (slot == -1)
    {
        *detailed_error_code = ERROR_TOO_MANY_OPEN_FILES;
        rc = FAIL_IO_RING_FILES_EXHAUSTED;
        goto exit;
    }
    if (slot >= IoRing.files_length && _grow_files(slot, detailed_error_code) == false)
    {
        rvn_slots_release(&IoRing.slots, slot);
        rc = FAIL_NOMEM;
        goto exit;
    }
    IoRing.files[slot] = registered;
    rc = _register_files(global_state->notify, detailed_error_code);
    if (rc != SUCCESS)
    {
        IoRing.files[slot] = IoRing.empty_file;
        rvn_slots_release(&IoRing.slots, slot);
        goto exit;
    }
    global_state->io_ring_file = slot;
    registered = NULL;
exit:
    ReleaseSRWLockExclusive(&IoRing.files_lock);
    if (registered != NULL)
        CloseHandle(registered);
    return rc;
}

/* Called once the last handle of the file is closed: no write of the file can be queued */
PRIVATE void
rvn_io_ring_unregister_file(struct handle_global_state *global_state)
{
    int32_t slot = global_state->io_ring_file;
    if (slot == -1)
        return;
    global_state->io_ring_file = -1;
    AcquireSRWLockExclusive(&IoRing.files_lock);
    HANDLE registered = IoRing.files[slot];
    IoRing.files[slot] = IoRing.empty_file;
    rvn_slots_release(&IoRing.slots, slot);
    int32_t ignored;
    (void)_register_files(global_state->notify, &ignored); // fails only when the ring is gone, and then it holds no file
    ReleaseSRWLockExclusive(&IoRing.files_lock);
    CloseHandle(registered);
}

