using System;
using System.Buffers;
using System.Globalization;
using System.Numerics;
using System.Text;
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
                    string number = reader.HasValueSequence ? Encoding.UTF8.GetString(reader.ValueSequence.ToArray()) : Encoding.UTF8.GetString(reader.ValueSpan.ToArray());
                    if (BigInteger.TryParse(number, NumberStyles.Integer, CultureInfo.InvariantCulture, out BigInteger integer))
                        return integer;

                    return reader.TryGetDecimal(out decimal decimalValue) ? new BigInteger(decimalValue) : new BigInteger(reader.GetDouble());
                case JsonTokenType.String:
                    return BigInteger.Parse(reader.GetString(), CultureInfo.InvariantCulture);
                default:
                    throw new JsonException($"Expected a number or a string for '{typeof(BigInteger).FullName}' but got {reader.TokenType}.");
            }
        }

        public override void Write(Utf8JsonWriter writer, BigInteger value, JsonSerializerOptions options)
        {
            throw new NotSupportedException(
                $"'{typeof(BigInteger).FullName}' values have no JSON representation in RavenDB documents. " +
                $"To store them, register a converter through {nameof(SystemTextJsonSerializationConventions)}.{nameof(SystemTextJsonSerializationConventions.CustomizeJsonSerializerOptions)} at index 0 of the converters list.");
        }
    }
}
