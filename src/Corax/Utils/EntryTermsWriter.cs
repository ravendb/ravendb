using System;
using Sparrow.Compression;
using Sparrow.Server;
using Voron.Util;

namespace Corax.Utils;

public unsafe struct EntryTermsWriter : IDisposable
{
    private readonly ByteStringContext _bsc;
    private ByteStringContext<ByteStringMemoryCache>.InternalScope _scope;
    private ByteString _bs;
    private int _offset;

    public EntryTermsWriter(ByteStringContext bsc)
    {
        _bsc = bsc;
        _scope = _bsc.Allocate(512, out _bs);
    }

    public int Encode(in NativeList<IndexWriter.RecordedTerm> terms)
    {
        const int maxItemSize = 32; // that should be more than enough to fit everything
        if (terms.Count * maxItemSize > _bs.Length)
        {
            _bsc.GrowAllocation(ref _bs, ref _scope, terms.Count * maxItemSize);
        }
        var buffer = _bs.Ptr;
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

    public void Dispose()
    {
        _scope.Dispose();
    }

    public void Write(Span<byte> space)
    {
        new Span<byte>(_bs.Ptr, _offset).CopyTo(space);
    }
}
