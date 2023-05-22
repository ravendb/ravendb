using System;
using System.Runtime.CompilerServices;
using Sparrow.Server;

namespace Voron.Util.Simd;

public unsafe struct SimdBufferedReader : IDisposable
{
    private long* _buffer;
    private int _bufferIdx, _usedBuffer;
    private readonly ByteStringContext _allocator;
    public SimdBitPacker<SortedDifferentials>.Reader Reader;
    private ByteStringContext<ByteStringMemoryCache>.InternalScope _bufferScope;

    public bool IsValid => Reader.IsValid;
    
    public SimdBufferedReader(ByteStringContext allocator, byte* p, int len)
    {
        _allocator = allocator;
        Reader = new(p, len);
        _buffer = null;
        _usedBuffer = 0;
        _bufferIdx = 0;
    }

    public int Fill(long* matches, int count)
    {
        while (true)
        {
            if (_bufferIdx != _usedBuffer)
            {
                var read = Math.Min(count, _usedBuffer - _bufferIdx);
                Unsafe.CopyBlock(matches, _buffer + _bufferIdx, (uint)(read * sizeof(long)));

                _bufferIdx += read;
                return read;
            }

            if (count < 256)
            {
                if (_buffer == null)
                {
                    _bufferScope = _allocator.Allocate(256 * sizeof(long), out var bs);
                    _buffer = (long*)bs.Ptr;
                }

                _bufferIdx = 0;
                _usedBuffer = Reader.Fill(_buffer, 256);
                if (_usedBuffer == 0) 
                    return 0;
                continue;
            }

            var sizeAligned = count & ~255;
            return Reader.Fill(matches, sizeAligned);
        }
    }

    public void Dispose()
    {
        _bufferScope.Dispose();
    }
}
