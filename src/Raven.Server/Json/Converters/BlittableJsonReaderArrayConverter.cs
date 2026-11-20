using System;
using System.Runtime.Serialization;
using System.Text.Json;
using System.Text.Json.Serialization;
using Sparrow.Json;

namespace Raven.Server.Json.Converters
{
    internal sealed class BlittableJsonReaderArrayConverter : JsonConverter<BlittableJsonReaderArray>
    {
        public static readonly BlittableJsonReaderArrayConverter Instance = new BlittableJsonReaderArrayConverter();

        private BlittableJsonReaderArrayConverter()
        {
        }

        public override void Write(Utf8JsonWriter writer, BlittableJsonReaderArray value, JsonSerializerOptions options)
        {
            RecordingJson.Write(writer, value);
        }

        public override BlittableJsonReaderArray Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            if (reader.TokenType != JsonTokenType.StartArray)
            {
                throw new SerializationException(
                    $"Can't convert {reader.TokenType} token to {nameof(BlittableJsonReaderArray)}. The value must to be an array");
            }

            return RecordingJson.ReadArray(ref reader);
        }
    }
}
