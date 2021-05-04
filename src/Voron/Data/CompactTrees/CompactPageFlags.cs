using System;

namespace Voron.Data.CompactTrees
{
    [Flags]
    public enum CompactPageFlags : byte
    {
        None = 0,
        Branch = 1,
        Leaf = 2
    }
}