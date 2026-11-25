using System;
using Sparrow.Platform;
using Sparrow.Server.Platform.Posix;

namespace Sparrow.Server.Platform;

public static class PalConfiguration
{
    // io_uring's largest file table (IORING_MAX_FIXED_FILES)
    private const int MaxRegisteredFiles = 1 << 20;

    /// <summary>
    /// The default size of a ring's file table: the process's limit on open files (RLIMIT_NOFILE) on Linux. Every registered
    /// data file also holds a descriptor, so the table cannot fill before the process runs out of descriptors.
    /// </summary>
    public static int FilesTheProcessCanOpen()
    {
        if (PlatformDetails.RunningOnLinux == false)
            return MaxRegisteredFiles; // no limit on open files to follow
        if (Syscall.getrlimit(Syscall.RLIMIT_NOFILE, out var limit) != 0)
            return 4096;
        return (int)Math.Min((ulong)limit.Current, MaxRegisteredFiles);
    }

    public static int IoRingFileTableSize => Math.Min(IoRingMaxFiles ?? int.MaxValue, FilesTheProcessCanOpen());

    public static int IoRingQueueSize = 32;
    public static int? IoRingMaxFiles; // null: FilesTheProcessCanOpen
    public static bool LowPriorityIo = false;
    public static Pal.RvnWriteMode WriteMode = Pal.RvnWriteMode.Auto;
    public static int PrefetchRingEntries = 256;
    public static int? PrefetchMaxFiles; // null: FilesTheProcessCanOpen
    public static int PrefetchQueueSize = 8192;

    // WriteMode as requested, before Pal resolved Auto into the mode in use
    public static Pal.RvnWriteMode RequestedWriteMode { get; internal set; } = Pal.RvnWriteMode.Auto;
}
