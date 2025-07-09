using System;
using System.Runtime.CompilerServices;
using Sparrow.Json;

namespace Raven.Server.Documents.SchemaValidation.ErrorMessage;

public class JsonOperationContextBuffer<T> : AbstractBuffer<T>
{
    private readonly JsonOperationContext _context;
    private AllocatedMemoryData _buffer;
    
    public JsonOperationContextBuffer(JsonOperationContext context)
    {
        _context = context;
    }

    protected override unsafe Span<T> BufferAsSpan()
    {
        return _buffer == null ? Span<T>.Empty : new Span<T>(_buffer.Address, _buffer.SizeInBytes / Unsafe.SizeOf<T>());
    }

    public override void CheckAndGrow(int minRequired)
    {
        minRequired *= Unsafe.SizeOf<T>();
        if (_buffer == null)
        {
            _buffer = _context.GetMemory(minRequired);
            return;
        }

        minRequired = Length * Unsafe.SizeOf<T>() + minRequired;
        if (minRequired <= _buffer.SizeInBytes)
            return;
        
        if (_context.GrowAllocation(_buffer, minRequired))
            return;

        var newBuffer = _context.GetMemory(minRequired);
        _buffer.AsSpan().CopyTo(newBuffer.AsSpan());
        _context.ReturnMemory(_buffer);
        _buffer = newBuffer;
    }
    
    public override void Dispose()
    {
        if(_buffer != null)
            _context.ReturnMemory(_buffer);
    }
}
