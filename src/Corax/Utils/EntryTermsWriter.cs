using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Text;
using Sparrow.Compression;
using Sparrow.Server;
using Voron.Impl;
using Voron.Util;

namespace Corax.Utils;

public unsafe struct EntryTermsWriter : IDisposable
{
    public const long NullMarker = ~long.MaxValue;
    
    private readonly ByteStringContext _bsc;
    private ByteStringContext<ByteStringMemoryCache>.InternalScope _scope;
    private ByteString _bs;
    private int _offset;

    public EntryTermsWriter(ByteStringContext bsc)
    {
        _bsc = bsc;
        _scope = _bsc.Allocate(512, out _bs);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void SetNullMarkerInTermContainerId(ref long termContainer)
    {
        termContainer |= NullMarker;
    }

    // We've essentially run out of free bits (we ensure that the three lowest bits are empty during encoding) to store field options (all used for other purposes).
    // However, since containers will never be negative, we can utilize the sign bit to store information about null values.
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static long GetTermContainerForNullFromRootPage(in long rootPage)
    {
        return (rootPage << 3) | NullMarker;
    }

    
    public int Encode(in NativeList<IndexWriter.RecordedTerm> terms)
    {
        const int maxItemSize = 32; // that should be more than enough to fit everything 
        if (terms.Count * maxItemSize > _bs.Length)
        {
            _bsc.GrowAllocation(ref _bs, ref _scope, terms.Count * maxItemSize);
        }
        var buffer = _bs.Ptr;
        // We sort the terms by the terms container id (to allow better delta compression)
        // Note that the flags are on the low bits, so won't interfere with the sort order
        // Stored fields are using a different term container id, which ensures that they have the 
        // same order after sorting as we encountered them, would typically be sorted first, but we don't rely on that
        // Spatial fields are using the fields root page as the term id, and will also tend to be first, again, not relying on that
        terms.Sort();
        int offset = 0;
        long prevTermId = 0;
        long prevLong = 0;
        for (int i = 0; i < terms.Count; i++)
        {
            ref var cur = ref terms.RawItems[i];

            // no need for zig/zag, since we are working on sorted values
            offset += VariableSizeEncoding.Write(buffer + offset, cur.TermContainerId - prevTermId);
            prevTermId = cur.TermContainerId;

            if ((cur.TermContainerId & 0b11) == 0b10) // stored / spatial field
            {
                if ((cur.TermContainerId & 0b100) != 0) // stored
                {
                    offset += ZigZagEncoding.Encode(buffer, cur.Long - prevLong,  offset);
                    prevLong = cur.Long;
                }
                else // spatial
                {
                    *(double*)(buffer + offset) = cur.Lat;
                    offset += sizeof(double);
                    *(double*)(buffer + offset) = cur.Lng;
                    offset += sizeof(double);
                }
                continue;
            }
            
            if (cur.HasLong == false) 
                continue;
            
            offset += ZigZagEncoding.Encode(buffer, cur.Long - prevLong,  offset);
            prevLong = cur.Long;

            if (cur.HasDouble == false) 
                continue;
            
            *(double*)(buffer + offset) = cur.Double;
            offset += sizeof(double);
        }

        _offset = offset;
        return offset;
    }
    
#if DEBUG
    public string Debug(LowLevelTransaction llt, in NativeList<IndexWriter.RecordedTerm> terms, IndexWriter r)
    {
        var fields = r.GetIndexedFieldNamesByRootPage();
        var sb = new StringBuilder();
        foreach (var term in terms.ToSpan())
        {
            if (term.TermContainerId < 0)
            {
                sb.Append($"NULL VAL");
                continue;
            }

            sb.Append($"+ {term.TermContainerId}");
        }

        return sb.ToString();
    }
#endif

    public void Dispose()
    {
        _scope.Dispose();
    }

    public void Write(Span<byte> space)
    {
        new Span<byte>(_bs.Ptr, _offset).CopyTo(space);
    }
}
