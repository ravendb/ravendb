using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Sparrow;
using Sparrow.Json;

namespace Raven.Server.SchemaValidation.ErrorMessage;

public class ErrorBuffer : IErrorBuffer
{
    private readonly AbstractBuffer<char> _buffer;
    
    public ErrorBuffer(AbstractBuffer<char> buffer)
    {
        _buffer = buffer;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ReadOnlySpan<char> AsSpan() => _buffer.AsSpan();

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public IErrorBuffer Append(string value)
    {
        _buffer.Append(value.AsSpan());
        return this;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public IErrorBuffer Append(ReadOnlySpan<char> value)
    {
        _buffer.Append(value);
        return this;
    }

    public IErrorBuffer Append(BlittableJsonReaderObject value)
    {
        if (value == null)
            return this;
        
        using (var memoryStream = new ErrorBufferStreamWrapper(this))
        {
            value.WriteJsonTo(memoryStream);
        }
        return this;
    }
    
    public IErrorBuffer Append(object value)
    {
        Debug.Assert(false, $"We should implement a dedicated Append to avoid string allocations {value.GetType().Name}");
        Append(value?.ToString());
        return this;
    }

    public IErrorBuffer Append(ISpanFormattable value)
    {
        int charsWritten;
        var toGrowIfFails = 16;
        while (value.TryFormat(_buffer.UnusedBuffer, out charsWritten, default, null) == false) // constrained call avoiding boxing for value types
        {
            _buffer.CheckAndGrow(toGrowIfFails);
            toGrowIfFails *= 2;
        }

        _buffer.Length += charsWritten;
        return this;
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public IErrorBuffer AppendUtf8(ReadOnlySpan<byte> value)
    {
        int minRequired = Encodings.Utf8.GetCharCount(value);
        _buffer.CheckAndGrow(minRequired);
        Span<char> bufferUnusedBuffer = _buffer.UnusedBuffer;
        _buffer.Length += Encodings.Utf8.GetChars(value, bufferUnusedBuffer);
        return this;
    }

    public override string ToString() => new string(_buffer.AsSpan());

    public void Dispose() => _buffer.Dispose();
}
