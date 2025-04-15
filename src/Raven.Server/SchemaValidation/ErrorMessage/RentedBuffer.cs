using System;
using System.Buffers;

namespace Raven.Server.SchemaValidation.ErrorMessage;

public class RentedBuffer<T> : AbstractBuffer<T>
{
    private const int MinimumArrayPoolLength = 256;

    private bool _isRented;
    private T[] _buffer;
    
    protected override Span<T> BufferAsSpan() => _buffer.AsSpan();

    public override void CheckAndGrow(int required)
    {
        if (_buffer == null)
        {
            _buffer = ArrayPool<T>.Shared.Rent(Math.Max(MinimumArrayPoolLength, required));
            _isRented = true;
            return;
        }

        required += Length;
        if (required < _buffer.Length)
            return;
        
        var newCapacity = Math.Max(required, _buffer.Length * 2);
        int arraySize = Math.Clamp(newCapacity, MinimumArrayPoolLength, int.MaxValue);

        var newArray = ArrayPool<T>.Shared.Rent(arraySize);
        _buffer.AsSpan(0, Length).CopyTo(newArray);

        if (_isRented)
            ArrayPool<T>.Shared.Return(_buffer);
        
        _buffer = newArray;
        _isRented = true;
    }
    
    public override void Dispose()
    {
        if (_isRented == false)
            return;
        ArrayPool<T>.Shared.Return(_buffer);
    }
}
