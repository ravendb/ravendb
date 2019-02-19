namespace Sparrow.Platform.Posix.macOS
{
    public unsafe struct host_cpu_load_info
    {
        public fixed uint cpu_ticks[(int)CpuState.CPU_STATE_MAX]; /* number of ticks while running... */
    }

    public enum CpuState
    {
        CPU_STATE_USER = 0,
        CPU_STATE_SYSTEM = 1,
        CPU_STATE_IDLE = 2,
        CPU_STATE_NICE = 3,
        CPU_STATE_MAX = 4,
    }
}
