using System;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Raven.Client.Json.Serialization.SystemTextJson.Internal.Converters
{
    internal sealed class CharConverter : JsonConverter<char>
    {
        private const int MaxUtf8BytesOfSingleChar = 12;

        public static readonly CharConverter Instance = new();

        private CharConverter()
        {
        }

        public override char Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            if (reader.TokenType == JsonTokenType.Number)
                return (char)reader.GetUInt16();

            if (reader.TokenType != JsonTokenType.String)
                throw new JsonException($"Expected a single character, got {reader.TokenType}.");

            long length = reader.HasValueSequence ? reader.ValueSequence.Length : reader.ValueSpan.Length;
            if (length > MaxUtf8BytesOfSingleChar)
                throw new JsonException($"Expected a single character, got '{reader.GetString()}'.");

            Span<char> chars = stackalloc char[MaxUtf8BytesOfSingleChar];
            int written = reader.CopyString(chars);
            if (written != 1)
                throw new JsonException($"Expected a single character, got '{chars.Slice(0, written).ToString()}'.");

            return chars[0];
        }

        public override void Write(Utf8JsonWriter writer, char value, JsonSerializerOptions options)
        {
            Span<char> chars = stackalloc char[1];
            chars[0] = value;
            writer.WriteStringValue(chars);
        }
    }
}
