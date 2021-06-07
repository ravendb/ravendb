using System;

namespace Voron.Data.BTrees
{
    [Flags]
    public enum TreeFlags : byte
    {
        None = 0,
        MultiValue = 1,
        FixedSizeTrees = 2,
        MultiValueTrees = 4,
        LeafsCompressed = 8,
        Streams = 16,
        CompactTrees = 32,
    }
}
