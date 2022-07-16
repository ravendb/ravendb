using System;
using System.Buffers;
using System.Collections.Generic;
using Voron;

namespace Corax.Utils;

public struct GrowablePooledBuffer<T> : IDisposable
{
    private T[] _buffer;
    private int _len;

    public GrowablePooledBuffer()
    {
        _len = 0;
        _buffer = Array.Empty<T>();
    }

    public void Ensure(int len)
    {
        if (_buffer.Length < len)
        {
            if(_buffer.Length != 0)
                ArrayPool<T>.Shared.Return(_buffer);
            _buffer = ArrayPool<T>.Shared.Rent(len);
        }
        _len = len;
    }

    public Span<T> Buffer => new Span<T>(_buffer, 0, _len);

    public void Dispose()
    {
        if(_buffer.Length != 0)
            ArrayPool<T>.Shared.Return(_buffer);
        _buffer = Array.Empty<T>();
        _len = 0;
    }

    public void Copy(ICollection<T> collection)
    {
        Ensure(collection.Count);
        collection.CopyTo(_buffer, 0);
    }
}
