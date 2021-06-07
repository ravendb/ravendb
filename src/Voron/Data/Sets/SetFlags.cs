using System;

namespace Voron.Data.Sets
{
    [Flags]
    public enum SetFlags : byte
    {
        None = 0,
        Branch = 1,
        Leaf = 2
    }
}
