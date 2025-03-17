using System;
using System.Buffers;
using System.Runtime.CompilerServices;

namespace Raven.Server.SchemaValidation;

public class RentedBuffer<T> : IDisposable
{
    private const int MinimumArrayPoolLength = 256;

    private bool _isRented;
    protected T[] Buffer;

    public int Length { get; protected set; }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Append(T value)
    {
        CheckAndGrow(1);
        Buffer[Length] = value;
        Length += 1;
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Append(ReadOnlySpan<T> value)
    {
        if (value.TryCopyTo(Buffer.AsSpan(Length)))
        {
            Length += value.Length;
        }
        else
        {
            CheckAndGrow(value.Length);
            value.CopyTo(Buffer.AsSpan(Length));
            Length += value.Length;
        }
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Trim(int count) => Length -= count;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ReadOnlySpan<T> AsSpan() => Buffer.AsSpan(0, Length);
    
    public void Dispose()
    {
        if (_isRented == false)
            return;
        ArrayPool<T>.Shared.Return(Buffer);
    }

    protected void CheckAndGrow(int required)
    {
        if (Buffer == null)
        {
            Buffer = ArrayPool<T>.Shared.Rent(Math.Max(MinimumArrayPoolLength, required));
            _isRented = true;
            return;
        }

        required += Length;
        if (required < Buffer.Length)
            return;
        
        var newCapacity = Math.Max(required, Buffer.Length * 2);
        int arraySize = Math.Clamp(newCapacity, MinimumArrayPoolLength, int.MaxValue);

        var newArray = ArrayPool<T>.Shared.Rent(arraySize);
        Buffer.AsSpan(0, Length).CopyTo(newArray);

        if (_isRented)
            ArrayPool<T>.Shared.Return(Buffer);
        
        Buffer = newArray;
        _isRented = true;
    }
}
