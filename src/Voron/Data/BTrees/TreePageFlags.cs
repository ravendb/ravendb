using System;

namespace Voron.Data.BTrees
{
    [Flags]
    public enum TreePageFlags : byte
    {
        None = 0,
        Branch = 1,
        Leaf = 2,
        Value = 4,
    }
}