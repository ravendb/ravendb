using System;
using System.Text.Json;
using System.Text.Json.Serialization;
using Sparrow;

namespace Raven.Client.Json.Serialization.SystemTextJson.Internal.Converters
{
    internal sealed class SizeConverter : JsonConverter<Size>
    {
        public static readonly SizeConverter Instance = new();

        private SizeConverter()
        {
        }

        public override Size Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            if (reader.TokenType != JsonTokenType.Number)
                throw new InvalidOperationException("Expected Number, Got " + reader.TokenType);

            return new Size(reader.GetInt64(), SizeUnit.Bytes);
        }

        public override void Write(Utf8JsonWriter writer, Size value, JsonSerializerOptions options)
        {
            writer.WriteNumberValue(value.GetValue(SizeUnit.Bytes));
        }
    }
}
