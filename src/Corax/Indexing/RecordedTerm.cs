using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Corax.Indexing;

[StructLayout(LayoutKind.Explicit, Pack = 1)]
public struct RecordedTerm : IComparable<RecordedTerm>
{
    [FieldOffset(0)]
    public long TermContainerId;
    [FieldOffset(8)]
    public long Long;
    [FieldOffset(16)]
    public double Double;

    [FieldOffset(8)]
    public double Lat;
    [FieldOffset(16)]
    public double Lng;
            
    // We take advantage of the fact that container ids always have the bottom bits cleared
    // to store metadata information here. Otherwise, we'll pay extra 8 bytes per term
    public bool HasLong => (TermContainerId & 1) == 1;
    public bool HasDouble => (TermContainerId & 2) == 2;

    public int CompareTo(RecordedTerm other)
    {
        return TermContainerId.CompareTo(other.TermContainerId);
    }

    public RecordedTerm(long termContainerId)
    {
        TermContainerId = termContainerId;
        Unsafe.SkipInit(out Long);
        Unsafe.SkipInit(out Double);
    }

    public RecordedTerm(long termContainerId, long @long)
    {
        TermContainerId = termContainerId;
        Long = @long;
    }

    public RecordedTerm(long termContainerId, double lat, double lng)
    {
        TermContainerId = termContainerId;
        Lat = lat;
        Lng = lng;
    }
}
