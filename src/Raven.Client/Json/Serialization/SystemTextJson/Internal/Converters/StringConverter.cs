using System;
using System.Globalization;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Raven.Client.Json.Serialization.SystemTextJson.Internal.Converters
{
    internal sealed class StringConverter : JsonConverter<string>
    {
        public static readonly StringConverter Instance = new();

        private StringConverter()
        {
        }

        public override string Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            switch (reader.TokenType)
            {
                case JsonTokenType.String:
                    return reader.GetString();
                case JsonTokenType.Number:
                    if (reader.TryGetInt64(out long longValue))
                        return longValue.ToString(CultureInfo.InvariantCulture);

                    if (reader.TryGetDecimal(out decimal decimalValue))
                        return decimalValue.ToString(CultureInfo.InvariantCulture);

                    return reader.GetDouble().ToString("R", CultureInfo.InvariantCulture);
                case JsonTokenType.True:
                    return bool.TrueString;
                case JsonTokenType.False:
                    return bool.FalseString;
                default:
                    throw new JsonException($"Cannot read a {reader.TokenType} token as a string.");
            }
        }

        public override void Write(Utf8JsonWriter writer, string value, JsonSerializerOptions options)
        {
            writer.WriteStringValue(value);
        }

        public override string ReadAsPropertyName(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            return reader.GetString();
        }

        public override void WriteAsPropertyName(Utf8JsonWriter writer, string value, JsonSerializerOptions options)
        {
            writer.WritePropertyName(value);
        }
    }
}
