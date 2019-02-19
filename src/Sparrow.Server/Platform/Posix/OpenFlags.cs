using System;

namespace Sparrow.Platform.Posix
{
    [Flags]
    
    public enum OpenFlags : int
    {
        //
        // One of these
        //
        O_RDONLY = 0x00000000,
        O_WRONLY = 0x00000001,
        O_RDWR = 0x00000002,

        //
        // Or-ed with zero or more of these
        //
        // O_CREAT = 0x00000040
        // O_EXCL = 0x00000080,
        // O_NOCTTY = 0x00000100,
        // O_TRUNC = 0x00000200,
        // O_APPEND = 0x00000400,
        // O_NONBLOCK = 0x00000800,
        // O_SYNC = 1052672, // 0x00101000, // value directly from printf("%d", O_SYNC)
        // O_DSYNC = 4096, // 0x00001000, // value directly from printf("%d", O_DSYNC)

        //
        // These are non-Posix.  Using them will result in errors/exceptions on
        // non-supported platforms.
        //
        // (For example, "C-wrapped" system calls -- calls with implementation in
        // MonoPosixHelper -- will return -1 with errno=EINVAL.  C#-wrapped system
        // calls will generate an exception in NativeConvert, as the value can't be
        // converted on the target platform.)
        //

        // O_NOFOLLOW = 0x00020000,
        // O_ASYNC = 0x00002000,
        // O_LARGEFILE = 0x00008000,
        // O_CLOEXEC = 0x00080000,
        // O_PATH = 0x00200000
    }
}
