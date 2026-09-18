using System;
using System.Text.Json;
using System.Text.Json.Serialization;
using Sparrow;

namespace Raven.Client.Json.Serialization.SystemTextJson.Internal.Converters
{
    internal sealed class SizeConverter : JsonConverterFactory
    {
        public static readonly SizeConverter Instance = new();

        private SizeConverter()
        {
        }

        public override bool CanConvert(Type typeToConvert)
        {
            return typeToConvert == typeof(Size) || typeToConvert == typeof(Size?);
        }

        public override JsonConverter CreateConverter(Type typeToConvert, JsonSerializerOptions options)
        {
            if (typeToConvert == typeof(Size))
                return SizeConverterInner.Instance;

            return NullableSizeConverter.Instance;
        }

        private sealed class SizeConverterInner : JsonConverter<Size>
        {
            public static readonly SizeConverterInner Instance = new();

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

        private sealed class NullableSizeConverter : JsonConverter<Size?>
        {
            public static readonly NullableSizeConverter Instance = new();

            public override Size? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
            {
                if (reader.TokenType == JsonTokenType.Null)
                    return null;

                if (reader.TokenType != JsonTokenType.Number)
                    throw new InvalidOperationException("Expected Number, Got " + reader.TokenType);

                return new Size(reader.GetInt64(), SizeUnit.Bytes);
            }

            public override void Write(Utf8JsonWriter writer, Size? value, JsonSerializerOptions options)
            {
                if (value == null)
                {
                    writer.WriteNullValue();
                    return;
                }

                writer.WriteNumberValue(value.Value.GetValue(SizeUnit.Bytes));
            }
        }
    }
}
