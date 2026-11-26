using System;
using System.Text.Json;
using Sparrow.Json;

namespace Raven.Client.Json.Serialization.SystemTextJson.Internal
{
    internal static class NamedFloatingPointLiterals
    {
        [ThreadStatic]
        private static bool _escaped;

        public static EscapeScope WriteEscaped()
        {
            bool previous = _escaped;
            _escaped = true;
            return new EscapeScope(previous);
        }

        public readonly struct EscapeScope(bool previous) : IDisposable
        {
            public void Dispose() => _escaped = previous;
        }

        public static void Write(Utf8JsonWriter writer, double value)
        {
            if (double.IsNaN(value))
                Write(writer, "NaN"u8, AbstractBlittableJsonTextWriter.EscapedNaNBuffer);
            else if (double.IsPositiveInfinity(value))
                Write(writer, "Infinity"u8, AbstractBlittableJsonTextWriter.EscapedPositiveInfinityBuffer);
            else
                Write(writer, "-Infinity"u8, AbstractBlittableJsonTextWriter.EscapedNegativeInfinityBuffer);
        }

        public static bool TryWriteEscaped(Utf8JsonWriter writer, LazyNumberValue value)
        {
            if (_escaped == false)
                return false;

            byte[] escaped = value.IsNaN() ? AbstractBlittableJsonTextWriter.EscapedNaNBuffer
                : value.IsPositiveInfinity() ? AbstractBlittableJsonTextWriter.EscapedPositiveInfinityBuffer
                : value.IsNegativeInfinity() ? AbstractBlittableJsonTextWriter.EscapedNegativeInfinityBuffer
                : null;

            if (escaped == null)
                return false;

            writer.WriteRawValue(escaped, skipInputValidation: true);
            return true;
        }

        private static void Write(Utf8JsonWriter writer, ReadOnlySpan<byte> literal, byte[] escaped)
        {
            writer.WriteRawValue(_escaped ? escaped : literal, skipInputValidation: true);
        }

        public static bool TryRead(ref Utf8JsonReader reader, out double value)
        {
            if (reader.TokenType == JsonTokenType.String && reader.ValueIsEscaped && reader.HasValueSequence == false)
            {
                ReadOnlySpan<byte> raw = reader.ValueSpan;
                if (IsQuotedBy(raw, AbstractBlittableJsonTextWriter.EscapedNaNBuffer))
                {
                    value = double.NaN;
                    return true;
                }

                if (IsQuotedBy(raw, AbstractBlittableJsonTextWriter.EscapedPositiveInfinityBuffer))
                {
                    value = double.PositiveInfinity;
                    return true;
                }

                if (IsQuotedBy(raw, AbstractBlittableJsonTextWriter.EscapedNegativeInfinityBuffer))
                {
                    value = double.NegativeInfinity;
                    return true;
                }
            }

            value = 0;
            return false;
        }

        private static bool IsQuotedBy(ReadOnlySpan<byte> raw, byte[] quoted)
        {
            return raw.SequenceEqual(quoted.AsSpan(1, quoted.Length - 2));
        }
    }
}
