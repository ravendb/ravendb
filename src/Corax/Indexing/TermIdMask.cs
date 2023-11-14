using System;

namespace Corax.Indexing;
// container ids are guaranteed to be aligned on 
// 4 bytes boundary, we're using this to store metadata
// about the data
[Flags]
public enum TermIdMask : long
{
    Single = 0,
        
    EnsureIsSingleMask = 0b11,
        
    SmallPostingList = 1,
    PostingList = 2
}
