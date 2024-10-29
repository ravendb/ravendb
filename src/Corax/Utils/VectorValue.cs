using System;
using System.Buffers;
using Sparrow.Server;
namespace Corax.Utils;

public struct VectorValue : IDisposable
{
    private readonly bool _isNative;
    private readonly ArrayPool<byte> _bufferPool;
    private byte[] _managedBuffer;
    private Memory<byte> _managedMemory;
    private ByteString _nativeMemory;
    private ByteStringContext<ByteStringMemoryCache>.InternalScope _nativeDisposal;
    private int _length;
    public int Length => _length;
    public ReadOnlySpan<byte> GetEmbedding()
    {
        if (_isNative)
        {
            return _nativeMemory.ToReadOnlySpan().Slice(0, _length);
        }
        
        return _managedMemory.IsEmpty
            ? _managedBuffer.AsSpan(0, _length) 
            : _managedMemory.Span.Slice(0, _length);
    }
    
    public VectorValue()
    {
    }

    public VectorValue(ArrayPool<byte> arrayPool, byte[] buffer, Memory<byte> embedding)
    {
        _isNative = false;
        _bufferPool = arrayPool;
        _managedBuffer = buffer;
        _managedMemory = embedding;
        _length = buffer?.Length ?? embedding.Length;
    }

    public VectorValue(ByteStringContext<ByteStringMemoryCache>.InternalScope nativeDisposal, ByteString nativeMemory, int usedBytes)
    {
        _isNative = true;
        _nativeDisposal = nativeDisposal;
        _nativeMemory = nativeMemory;
        _length = usedBytes;
    }
    
    public void OverrideLength(int len) => _length = len;
    

    public void Dispose()
    {
        if (_isNative)
        {
            if (_nativeMemory.HasValue)
            {
                _nativeDisposal.Dispose();
                _nativeDisposal = default;
                _nativeMemory = default;
            }
            return;
        }

        //either disposed or from managed
        if (_bufferPool == null) 
            return;
        
        _bufferPool.Return(_managedBuffer);
        _managedBuffer = null;
        _managedMemory = null;
    }
}
