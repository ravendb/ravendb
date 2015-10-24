using System;

namespace Voron.Data.BTrees
{
    [Flags]
    public enum PageFlags : byte
    {
        Single = 1,
        Overflow = 2,
        VariableSizeTreePage = 4,
        FixedSizeTreePage = 8,
        ZFastTreePage = 16,
        Reserved1 = 32,
        Reserved2 = 64,
        Reserved3 = 128,
    }

    [Flags]
    public enum TreePageFlags : byte
    {
        None = 0,
        Branch = 1,
        Leaf = 2,
        Value = 4,
    }

    [Flags]
    public enum FixedSizeTreePageFlags : byte
    {
        None = 0,
        Branch = 1,
        Leaf = 2,
        Value = 4,
    }
}