namespace Sparrow.Platform.Posix.macOS
{
    internal enum Flavor
    {
        // host_statistics()
        HOST_LOAD_INFO = 1,         /* System loading stats */
        HOST_VM_INFO = 2,           /* Virtual memory stats */
        HOST_CPU_LOAD_INFO = 3,     /* CPU load stats */

        // host_statistics64()
        HOST_VM_INFO64 = 4,         /* 64-bit virtual memory stats */
        HOST_EXTMOD_INFO64 = 5,     /* External modification stats */
        HOST_EXPIRED_TASK_INFO = 6  /* Statistics for expired tasks */
    }
}
