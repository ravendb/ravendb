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
        // run out of bits, the actual type of this is specified
        // in byte #13 of the page header
        Other = 128,
    }
    
    public enum ExtendedPageType : byte
    {
        None = 0,
        PostingListLeaf = 1,
        PostingListBranch = 2,
        Container = 3,
        ContainerOverflow = 4,
    }
}
