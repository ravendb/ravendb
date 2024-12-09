#include "rvn.h"
#include <assert.h>
#include "status_codes.h"
#include <stdio.h>
#include <windows.h>

//void main() {
//    char buffer[MAX_PATH];
//    GetCurrentDirectory(MAX_PATH, buffer);
//    printf("%s\n", buffer);
//    void* handle;
//    void* mem;
//    void* wmem;
//    int64_t size;
//    int32_t err;
//    int rc = rvn_open_journal_for_writes(L"test.waj", JOURNAL_MODE_PURE_MEMORY, 1024 * 64, DURABILITY_SUPPORTED, &handle, &size, &err);
//
//    char* b = VirtualAlloc(NULL, 1024 * 1024 * 8, MEM_COMMIT | MEM_RESERVE, PAGE_READWRITE);
//    memset(b, 'b', 1024 *  8);
//    for (size_t i = 0; i < 1024 * 1024 *  8; i += 80)
//    {
//        b[i] = '\r';
//        b[i + 1] = '\n';
//    }
//
//    char* c = VirtualAlloc(NULL, 1024 * 1024 * 8, MEM_COMMIT | MEM_RESERVE, PAGE_READWRITE);
//    memset(c, 'c', 1024 * 8);
//    for (size_t i = 0; i < 1024 * 1024 *  8; i += 80)
//    {
//        c[i] = '\r';
//        c[i + 1] = '\n';
//    }
//
//    struct journal_entry entries[2] = {
//        {.base = b, .number_of_4kbs = 1024 * 1024 * 8 / 4096 },
//        {.base = c, .number_of_4kbs = 1024 * 1024 * 8 / 4096 }
//    };
//
//    rc = rvn_write_journal(handle, entries, 2, 0, &err);
//
//
//}


// Function to be executed in parallel
VOID CALLBACK MyTask(PTP_CALLBACK_INSTANCE Instance, PVOID Parameter, PTP_WORK Work)
{
    void* jrnl;
    void* handle;
    void* mem;
    void* wmem;
    int64_t size;
    int32_t err;
    int rc;
    WCHAR data[20];
    WCHAR journal[20];
    int i = (int)Parameter;
    printf("Task %d\n", i);
    swprintf(data, 10, L"%d.data", i);
    swprintf(journal, 10, L"%d.jrnl", i);

    rc = rvn_init_pager(data, 1024 * 64, OPEN_FILE_NONE, &handle, &mem, &wmem, &size, &err);
    assert(rc == SUCCESS);
    rc = rvn_open_journal_for_writes(journal, JOURNAL_MODE_PURE_MEMORY, 1024 * 64, DURABILITY_SUPPORTED, &jrnl, &size, &err);
    assert(rc == SUCCESS);
    rc = rvn_close_journal(jrnl, &err);
    assert(rc == SUCCESS);
    rc = rvn_close_pager(handle, &err);
    assert(rc == SUCCESS);

}

int main()
{
    // Create a thread pool
    PTP_POOL pool = CreateThreadpool(NULL);
    if (pool == NULL) {
        printf("Failed to create thread pool\n");
        return 1;
    }

    // Set the maximum number of threads in the pool
    SetThreadpoolThreadMaximum(pool, 4);

    // Create a cleanup group
    PTP_CLEANUP_GROUP cleanupGroup = CreateThreadpoolCleanupGroup();
    if (cleanupGroup == NULL) {
        printf("Failed to create cleanup group\n");
        CloseThreadpool(pool);
        return 1;
    }

    // Associate the pool and cleanup group with a callback environment
    TP_CALLBACK_ENVIRON env;
    InitializeThreadpoolEnvironment(&env);
    SetThreadpoolCallbackPool(&env, pool);
    SetThreadpoolCallbackCleanupGroup(&env, cleanupGroup, NULL);

    // Submit tasks to the thread pool
    for (int i = 0; i < 1000; i++) {
        PTP_WORK work = CreateThreadpoolWork(MyTask, i, &env);
        if (work == NULL) {
            printf("Failed to create work for task %d\n", i);
            continue;
        }
        SubmitThreadpoolWork(work);
    }

    // Wait for all tasks to complete
    CloseThreadpoolCleanupGroupMembers(cleanupGroup, FALSE, NULL);
    CloseThreadpoolCleanupGroup(cleanupGroup);
    DestroyThreadpoolEnvironment(&env);
    CloseThreadpool(pool);

    return 0;
}

