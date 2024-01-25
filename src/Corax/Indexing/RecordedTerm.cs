using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Corax.Utils;
using Voron.Util;

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

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static RecordedTerm CreateForStored<T>(in NativeList<T> fieldTerms, in StoredFieldType storedFieldType, in long listContainerId)
        where T : unmanaged
    {
        return new RecordedTerm
        (
            // why: entryTerms.Count << 8 
            // we put entries count here because we are sorting the entries afterward
            // this ensure that stored values are then read using the same order we have for writing them
            // which is important for storing arrays
            termContainerId: fieldTerms.Count << Constants.IndexWriter.TermFrequencyShift | (int)storedFieldType | 0b110, // marker for stored field
            @long: listContainerId
        );
    }
}
