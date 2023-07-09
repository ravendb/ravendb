using System;
using System.Runtime.CompilerServices;
using System.Text;
using Sparrow.Server;

namespace Voron.Util.PFor;



public unsafe struct FastPForBufferedReader : IDisposable
{
    private long* _buffer;
    private int _bufferIdx, _usedBuffer;
    private readonly ByteStringContext _allocator;
    public FastPForDecoder Decoder;
    private ByteStringContext<ByteStringMemoryCache>.InternalScope _bufferScope;
    private const int InternalBufferSize = 256;

    public bool IsValid => Decoder.IsValid;
    
    public FastPForBufferedReader(ByteStringContext allocator, byte* p, int len)
    {
        _allocator = allocator;
        if (len > 0)
        {
            Decoder = new FastPForDecoder(_allocator);
            Decoder.Init(p, len);
        }
        else
        {
            Decoder = default;
        }
        _buffer = null;
        _usedBuffer = 0;
        _bufferIdx = 0;
    }

    public int Fill(long* matches, int count)
    {
        while (Decoder.IsValid)
        {
            if (_bufferIdx != _usedBuffer)
            {
                var read = Math.Min(count, _usedBuffer - _bufferIdx);
                Unsafe.CopyBlock(matches, _buffer + _bufferIdx, (uint)(read * sizeof(long)));

                _bufferIdx += read;
                return read;
            }

            if (count < InternalBufferSize)
            {
                if (_buffer == null)
                {
                    _bufferScope = _allocator.Allocate(InternalBufferSize * sizeof(long), out var bs);
                    _buffer = (long*)bs.Ptr;
                }

                _bufferIdx = 0;
                _usedBuffer = Decoder.Read(_buffer, InternalBufferSize);
                if (_usedBuffer == 0) 
                    return 0;
                continue;
            }

            var sizeAligned = count & ~255;
            return Decoder.Read(matches, sizeAligned);
        }

        return 0;
    }

    public void Dispose()
    {
        _bufferScope.Dispose();
        Decoder.Dispose();
    }
}
