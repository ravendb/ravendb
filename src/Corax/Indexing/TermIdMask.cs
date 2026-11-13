using System;

namespace Corax.Indexing;

// container ids are guaranteed to be aligned on 4 bytes boundary, we're using this 
// to store metadata about the data using the bottom 2 bits
[Flags]
public enum TermIdMask : long
{
    Single            = 0b00,
    SmallPostingList  = 0b01,
    PostingList       = 0b10,
    
    // 0b11 is never produced for a real term. Callers use it as a synthetic "this is not a real posting list" marker
    Reserved          = 0b11,
    
    EnsureIsSingleMask = 0b11,
}
