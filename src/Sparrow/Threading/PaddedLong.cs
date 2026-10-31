using System.Runtime.InteropServices;

namespace Sparrow.Threading;

// a long in its own cache line, for the per-core slots that many cores update (a nested type of a generic class cannot
// have an explicit layout, so it is not nested in them)
[StructLayout(LayoutKind.Explicit, Size = 64)]
internal struct PaddedLong
{
    // at the end of the 64 bytes: every indexing reads the array's length (bounds check), which is 8 bytes before
    // the first element, so a value at the start of the first slot would share that line and each write to it
    // would invalidate the line on all the cores
    [FieldOffset(56)]
    public long Value;
}
