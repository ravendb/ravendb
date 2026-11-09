using System;
using System.Buffers;
using System.Globalization;
using System.Numerics;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Raven.Client.Json.Serialization.SystemTextJson.Internal.Converters
{
    internal sealed class BigIntegerConverter : JsonConverter<BigInteger>
    {
        public static readonly BigIntegerConverter Instance = new();

        private BigIntegerConverter()
        {
        }

        public override BigInteger Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            switch (reader.TokenType)
            {
                case JsonTokenType.Number:
                    if (reader.TryGetInt64(out long int64))
                        return int64;

                    if (reader.TryGetUInt64(out ulong uint64))
                        return uint64;

                    if (TryParseInteger(ref reader, out BigInteger integer))
                        return integer;

                    return reader.TryGetDecimal(out decimal decimalValue) ? new BigInteger(decimalValue) : new BigInteger(reader.GetDouble());
                case JsonTokenType.String:
                    return BigInteger.Parse(reader.GetString(), CultureInfo.InvariantCulture);
                default:
                    throw new JsonException($"Expected a number or a string for '{typeof(BigInteger).FullName}' but got {reader.TokenType}.");
            }
        }

        private static bool TryParseInteger(ref Utf8JsonReader reader, out BigInteger integer)
        {
            ReadOnlySpan<byte> utf8 = reader.HasValueSequence ? reader.ValueSequence.ToArray() : reader.ValueSpan;
            Span<char> chars = utf8.Length <= 256 ? stackalloc char[utf8.Length] : new char[utf8.Length];
            for (int i = 0; i < utf8.Length; i++)
                chars[i] = (char)utf8[i];

#if NETSTANDARD2_0
            return BigInteger.TryParse(chars.ToString(), NumberStyles.Integer, CultureInfo.InvariantCulture, out integer);
#else
            return BigInteger.TryParse(chars, NumberStyles.Integer, CultureInfo.InvariantCulture, out integer);
#endif
        }

        public override void Write(Utf8JsonWriter writer, BigInteger value, JsonSerializerOptions options)
        {
            throw new NotSupportedException(
                $"'{typeof(BigInteger).FullName}' values have no JSON representation in RavenDB documents. " +
                $"To store them, register a converter through {nameof(SystemTextJsonSerializationConventions)}.{nameof(SystemTextJsonSerializationConventions.CustomizeJsonSerializerOptions)}.");
        }
    }
}
