using System;
using System.Runtime.CompilerServices;
using Sparrow.Server;

namespace Raven.Server.Documents.SchemaValidation.ErrorMessage;

public class ByteStringContextBuffer<T> : AbstractBuffer<T> where T : unmanaged
{
    private readonly ByteStringContext _allocator;
    private ByteString _buffer;
    private ByteStringContext<ByteStringMemoryCache>.InternalScope _bufferScope;
    
    public ByteStringContextBuffer(ByteStringContext allocator)
    {
        _allocator = allocator;
    }

    protected override Span<T> BufferAsSpan() => _buffer.Length == 0 ? Span<T>.Empty : _buffer.ToSpan<T>();

    public override void CheckAndGrow(int minRequired)
    {
        minRequired *= Unsafe.SizeOf<T>();
        if (_buffer.Length == 0)
        {
            _bufferScope = _allocator.Allocate(minRequired, out _buffer);
            return;
        }

        if (Length * Unsafe.SizeOf<T>() + minRequired <= _buffer.Length)
            return;

        _allocator.GrowAllocation(ref _buffer, ref _bufferScope, minRequired);
    }
    
    public override void Dispose()
    {
        _bufferScope.Dispose();
    }
}
