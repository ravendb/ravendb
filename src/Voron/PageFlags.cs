using System;

namespace Voron
{
    [Flags]
    public enum PageFlags : byte
    {
        Single = 1,
        Overflow = 2,
        VariableSizeTreePage = 4,
        FixedSizeTreePage = 8,
        Stream = 16,
        RawData = 32,
        Compressed = 64,
        SetPage = 128,
    }
}
