using System;
using System.Runtime.CompilerServices;

namespace Raven.Server.Documents.SchemaValidation.ErrorMessage;

public abstract class AbstractBuffer<T> : IDisposable
{
    public int Length { get; set; }

    public Span<T> UnusedBuffer => BufferAsSpan()[Length..];
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Append(ReadOnlySpan<T> value)
    {
        CheckAndGrow(value.Length);
        value.CopyTo(UnusedBuffer);
        Length += value.Length;
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Append(T value)
    {
        CheckAndGrow(1);
        UnusedBuffer[0] = value;
        Length += 1;
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Trim(int count) => Length -= count;
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ReadOnlySpan<T> AsSpan() => BufferAsSpan()[..Length];

    protected abstract Span<T> BufferAsSpan();

    public abstract void CheckAndGrow(int minRequired);
    
    public void Reset() => Length = 0;
    
    public abstract void Dispose();
}
