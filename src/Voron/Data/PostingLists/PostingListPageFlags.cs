using System;

namespace Voron.Data.PostingLists
{
    [Flags]
    public enum PostingListPageFlags : byte
    {
        None = 0,
        Branch = 1,
        Leaf = 2
    }
}
