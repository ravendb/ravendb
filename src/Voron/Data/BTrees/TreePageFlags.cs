using System;
using Voron.Data;

namespace Voron.Data.BTrees
{
    [Flags]
    public enum TreePageFlags : byte
    {
        None = 0,
        Branch = 1,
        Leaf = 2,
        Value = 4,
        Reserved = PageCollapsedLevels.Mask, // not available here, PageCollapsedLevels keeps the count in them
    }
}