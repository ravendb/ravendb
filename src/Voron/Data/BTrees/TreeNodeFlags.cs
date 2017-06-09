using System;

namespace Voron.Data.BTrees
{
    [Flags]
    public enum TreeNodeFlags : byte
    {
        Data = 1,
        PageRef = 2,
        MultiValuePageRef = 3,
        CompressionTombstone = 4,
        
        // Non persistent flags
        NewOnly = 128
    }
}