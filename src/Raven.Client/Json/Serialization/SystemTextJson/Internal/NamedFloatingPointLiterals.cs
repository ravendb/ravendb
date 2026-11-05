using System;
using System.Text.Json;
using Sparrow.Json;

namespace Raven.Client.Json.Serialization.SystemTextJson.Internal
{
    internal static class NamedFloatingPointLiterals
    {
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
