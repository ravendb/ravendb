using System;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Raven.Client.Json.Serialization.SystemTextJson.Internal.Converters
{
    internal sealed class BooleanConverter : JsonConverter<bool>
    {
        public static readonly BooleanConverter Instance = new();

        private BooleanConverter()
        {
        }

        public override bool Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            switch (reader.TokenType)
            {
                case JsonTokenType.True:
                    return true;
                case JsonTokenType.False:
                    return false;
                case JsonTokenType.String:
                    return bool.Parse(reader.GetString());
                case JsonTokenType.Number:
                    return reader.GetDouble() != 0;
                default:
                    throw new JsonException($"Cannot read a {reader.TokenType} token as a boolean.");
            }
        }

        public override void Write(Utf8JsonWriter writer, bool value, JsonSerializerOptions options)
        {
            writer.WriteBooleanValue(value);
        }

        public override bool ReadAsPropertyName(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            return bool.Parse(reader.GetString());
        }

        public override void WriteAsPropertyName(Utf8JsonWriter writer, bool value, JsonSerializerOptions options)
        {
            writer.WritePropertyName(value ? bool.TrueString : bool.FalseString);
        }
    }
}
