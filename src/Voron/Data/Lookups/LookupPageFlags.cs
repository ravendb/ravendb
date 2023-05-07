using System;

namespace Voron.Data.Lookups
{
    [Flags]
    public enum LookupPageFlags : byte
    {
        None = 0,
        Branch = 1,
        Leaf = 2
    }
}
