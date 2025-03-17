using System;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using Sparrow;

namespace Raven.Server.SchemaValidation;

public class RentedCharBuffer : RentedBuffer<char>
{
    public override string ToString() => new string(Buffer.AsSpan(0, Length));

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Append(string value) => Append(value.AsSpan());

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public new void Append(ReadOnlySpan<char> value) => base.Append(value.ToString());

    public void Append<T>(T value)
    {
        switch (value)
        {
            case IFormattable formattable:
            {
                if (formattable is ISpanFormattable spanFormattable)
                {
                    int charsWritten;
                    while (!spanFormattable.TryFormat(Buffer.AsSpan(Length), out charsWritten, default, null)) // constrained call avoiding boxing for value types
                    {
                        CheckAndGrow(1);
                    }

                    Length += charsWritten;
                    return;
                }

                Debug.Assert(false, "We should implement a dedicated Append to avoid string allocations");
                Append(formattable.ToString(format: null, null)); // constrained call avoiding boxing for value types
                break;
            }
            case string stringValue:
                Append(stringValue);
                break;
            default:
                Debug.Assert(false, "We should implement a dedicated Append to avoid string allocations");
                Append(value?.ToString());
                break;
        }
    }

    public void Read(StreamReader streamReader)
    {
        do
        {
            var read = streamReader.Read(Buffer.AsSpan(Length));
            Length += read;
        } while (streamReader.EndOfStream == false);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void AppendUtf8(Span<byte> asSpan)
    {
        Length += Encodings.Utf8.GetChars(asSpan, Buffer.AsSpan(Length));
    }
}
