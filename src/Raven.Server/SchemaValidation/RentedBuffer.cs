using System;
using System.Buffers;
using System.Runtime.CompilerServices;

namespace Raven.Server.SchemaValidation;

public class RentedBuffer<T> : IDisposable
{
    private const int MinimumArrayPoolLength = 256;

    private T[] _arrayToReturnToPool;

    protected T[] ArrayToUse;

    public int Length { get; protected set; }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Append(ReadOnlySpan<T> value)
    {
        if (value.TryCopyTo(ArrayToUse.AsSpan(Length)))
        {
            Length += value.Length;
        }
        else
        {
            CheckAndGrow(value.Length);
            value.CopyTo(ArrayToUse.AsSpan(Length));
            Length += value.Length;
        }
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Trim(int count) => Length -= count;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ReadOnlySpan<T> AsSpan() => ArrayToUse.AsSpan(0, Length);
    
    public void Dispose()
    {
        var toReturn = _arrayToReturnToPool;
        if (toReturn is null)
            return;
        ArrayPool<T>.Shared.Return(toReturn);
    }

    protected void CheckAndGrow(int required)
    {
        if (ArrayToUse == null)
        {
            ArrayToUse = _arrayToReturnToPool = ArrayPool<T>.Shared.Rent(Math.Max(MinimumArrayPoolLength, required));
            return;
        }
        
        if (required < ArrayToUse.Length)
            return;
        
        var newCapacity = Math.Max(required, ArrayToUse.Length * 2);
        int arraySize = Math.Clamp(newCapacity, MinimumArrayPoolLength, int.MaxValue);

        var newArray = ArrayPool<T>.Shared.Rent(arraySize);
        ArrayToUse.AsSpan(0, Length).CopyTo(newArray);

        var toReturn = _arrayToReturnToPool;
        ArrayToUse = _arrayToReturnToPool = newArray;

        if (toReturn is not null)
        {
            ArrayPool<T>.Shared.Return(toReturn);
        }
    }
}
