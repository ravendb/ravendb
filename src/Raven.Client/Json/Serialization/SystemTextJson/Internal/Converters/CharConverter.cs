using System;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Raven.Client.Json.Serialization.SystemTextJson.Internal.Converters
{
    internal sealed class CharConverter : JsonConverter<char>
    {
        public static readonly CharConverter Instance = new();

        private CharConverter()
        {
        }

        public override char Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            if (reader.TokenType == JsonTokenType.Number)
                return (char)reader.GetUInt16();

            string value = reader.GetString();
            if (value == null || value.Length != 1)
                throw new JsonException($"Expected a single character, got '{value}'.");

            return value[0];
        }

        public override void Write(Utf8JsonWriter writer, char value, JsonSerializerOptions options)
        {
            writer.WriteStringValue(value.ToString());
        }
    }
}
