using System;
using Voron.Data;

namespace Voron.Data.Lookups
{
    [Flags]
    public enum LookupPageFlags : byte
    {
        None = 0,
        Branch = 1,
        Leaf = 2,
        Reserved = PageCollapsedLevels.Mask, // not available here, PageCollapsedLevels keeps the count in them
    }
}
