// required to enable io ring support
#define NTDDI_VERSION NTDDI_WIN10_NI

#include <windows.h>
#include <VersionHelpers.h>
#include <stdio.h>
#include <ioringapi.h>

#include "rvn.h"
#include "rvn_internal.h"
#include "status_codes.h"
#include "internal_win.h"

typedef HRESULT(WINAPI *PFN_QueryIoRingCapabilities)(IORING_CAPABILITIES *);
typedef HRESULT(WINAPI *PFN_CloseIoRing)(HIORING);
typedef HRESULT(WINAPI *PFN_CreateIoRing)(IORING_VERSION, IORING_CREATE_FLAGS, UINT32, UINT32, HIORING *);
typedef HRESULT(WINAPI *PFN_SubmitIoRing)(HIORING, UINT32, UINT32, UINT32 *);
typedef HRESULT(WINAPI *PFN_PopIoRingCompletion)(HIORING, IORING_CQE *);
typedef HRESULT(WINAPI *PFN_BuildIoRingWriteFile)(HIORING, IORING_HANDLE_REF, IORING_BUFFER_REF, UINT32, UINT64, FILE_WRITE_FLAGS, UINT_PTR, IORING_SQE_FLAGS);
typedef HRESULT(WINAPI *PFN_SetIoRingCompletionEvent)(HIORING, HANDLE);

typedef enum workitem_type
{
    workitem_none,
    workitem_write,
    workitem_fsync,
} workitem_type;

struct submittion
{
    HANDLE file;
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

    HIORING io_ring;
    HANDLE event;
    HANDLE thread;
    HANDLE has_error;
    int errored;
    struct workitem *head;
};

struct IoRingSetup IoRing;

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

    FreeLibrary(hKernelDll);

    return (s->QueryIoRingCapabilities &&
            s->CloseIoRing &&
            s->CreateIoRing &&
            s->SubmitIoRing &&
            s->PopIoRingCompletion &&
            s->SetIoRingCompletionEvent &&
            s->BuildIoRingWriteFile);
}

void queue_work(struct workitem *head, struct workitem *last)
{
    while (true)
    {
        struct workitem *cur_head = IoRing.head;
        last->next = cur_head;
        if (InterlockedCompareExchangePointer(&IoRing.head, head, cur_head) == cur_head)
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
        work->submittion->result = hr;
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
                work = InterlockedExchangePointer(&IoRing.head, 0);
            }
            while (work)
            {
                has_work = true;
                IORING_HANDLE_REF file_handle_ref = IoRingHandleRefFromHandle(work->submittion->file);
                IORING_BUFFER_REF buffer_ref = IoRingBufferRefFromPointer(work->buffer);
                int32_t size_to_write = (int32_t)(rvn_min(work->size, INT32_MAX));
                hr = IoRing.BuildIoRingWriteFile(ring, file_handle_ref,
                                                 buffer_ref, size_to_write, work->offset, FILE_WRITE_FLAGS_NONE,
                                                 (UINT_PTR)work, IOSQE_FLAGS_NONE);

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

            struct submittion *completed = NULL;
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
                        cur->submittion->result = cqe.ResultCode;
                    }
                    break;

                default:
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
        work = InterlockedExchangePointer(&IoRing.head, 0);
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
        .file = handle_ptr->file_handle,
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
    
    int rc = SUCCESS;
    IORING_CREATE_FLAGS flags = {0};
    HRESULT hr = IoRing.CreateIoRing(IORING_VERSION_3, flags, g_cfg.io_ring_queue_size, g_cfg.io_ring_queue_size * 2, &IoRing.io_ring);
    if (FAILED(hr))
    {
        *detailed_error_code = GetLastError();
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
        *detailed_error_code = hr;
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
    return rc;
}

int io_ring_setup_successful()
{
    return IoRing.io_ring != NULL;
}
