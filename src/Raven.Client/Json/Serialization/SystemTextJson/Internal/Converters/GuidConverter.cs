using System;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Raven.Client.Json.Serialization.SystemTextJson.Internal.Converters
{
    internal sealed class GuidConverter : JsonConverter<Guid>
    {
        public static readonly GuidConverter Instance = new();

        private GuidConverter()
        {
        }

        public override Guid Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            if (reader.TokenType != JsonTokenType.String)
                throw new JsonException($"Expected a string for '{typeof(Guid).FullName}' but got {reader.TokenType}.");

            return reader.TryGetGuid(out Guid guid) ? guid : new Guid(reader.GetString());
        }

        public override void Write(Utf8JsonWriter writer, Guid value, JsonSerializerOptions options)
        {
            writer.WriteStringValue(value);
        }

        public override Guid ReadAsPropertyName(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            return new Guid(reader.GetString());
        }

        public override void WriteAsPropertyName(Utf8JsonWriter writer, Guid value, JsonSerializerOptions options)
        {
            writer.WritePropertyName(value.ToString("D"));
        }
    }
}
