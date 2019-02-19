using System;

namespace Sparrow.Platform.Posix
{
    [Flags]
    public enum MAdvFlags : int
    {
        MADV_NORMAL = 0x0,  /* No further special treatment */
        MADV_RANDOM = 0x1, /* Expect random page references */
        MADV_SEQUENTIAL = 0x2,  /* Expect sequential page references */
        MADV_WILLNEED = 0x3, /* Will need these pages */
        MADV_DONTNEED = 0x4, /* Don't need these pages */
        MADV_FREE = 0x5, /* Contents can be freed */
        MADV_ACCESS_DEFAULT = 0x6, /* default access */
        MADV_ACCESS_LWP = 0x7, /* next LWP to access heavily */
        MADV_ACCESS_MANY = 0x8, /* many processes to access heavily */
    }
}
