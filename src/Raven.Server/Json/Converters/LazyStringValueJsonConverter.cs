using System;
using System.Runtime.Serialization;
using System.Text.Json;
using System.Text.Json.Serialization;
using Sparrow.Json;

namespace Raven.Server.Json.Converters
{
    internal sealed class LazyStringValueJsonConverter : JsonConverter<LazyStringValue>
    {
        public static readonly LazyStringValueJsonConverter Instance = new LazyStringValueJsonConverter();

        private LazyStringValueJsonConverter()
        {
        }

        public override void Write(Utf8JsonWriter writer, LazyStringValue value, JsonSerializerOptions options)
        {
            RecordingJson.Write(writer, value);
        }

        public override LazyStringValue Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            if (reader.TokenType != JsonTokenType.String)
                throw new SerializationException($"Try to read {nameof(LazyStringValue)} from {reader.TokenType} token. Should be string here");

            return RecordingJson.Context.GetLazyString(reader.GetString());
        }
    }
}
