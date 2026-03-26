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

    public unsafe int Append(int alreadySeen, UnmanagedWriteBuffer buffer)
    {
        // alreadySeen is bytes offset - the number of bytes that have already been seen in the buffer. We need to append the remaining bytes.
        if (alreadySeen < 0)
            throw new ArgumentOutOfRangeException(nameof(alreadySeen));

        if (alreadySeen >= buffer.SizeInBytes)
            return 0;

        var bytesToAdd = buffer.SizeInBytes - alreadySeen;

        var elementSize = Unsafe.SizeOf<T>();

        if (bytesToAdd % elementSize != 0)
            throw new InvalidOperationException(
                $"Cannot append {bytesToAdd} bytes — not aligned to element size {elementSize}.");

        var elementsToAdd = bytesToAdd / elementSize;

        CheckAndGrow(Length + elementsToAdd);

        buffer.CopyTo(
            start: alreadySeen,
            pointer: _buffer.Address + Length * elementSize);

        Length += elementsToAdd;

        return bytesToAdd;
    }

    public Memory<byte> AsMemory() => _buffer.AsMemory()[..Length];

    public override void Dispose()
    {
        if (_buffer != null)
            _context.ReturnMemory(_buffer);
    }
}
