using System;
using System.Runtime.Serialization;
using System.Text.Json;
using System.Text.Json.Serialization;
using Sparrow.Json;

namespace Raven.Server.Json.Converters
{
    internal sealed class BlittableJsonConverter : JsonConverter<BlittableJsonReaderObject>
    {
        public static readonly BlittableJsonConverter Instance = new BlittableJsonConverter();

        private BlittableJsonConverter()
        {
        }

        public override void Write(Utf8JsonWriter writer, BlittableJsonReaderObject value, JsonSerializerOptions options)
        {
            RecordingJson.Write(writer, value);
        }

        public override BlittableJsonReaderObject Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            if (reader.TokenType != JsonTokenType.StartObject)
            {
                throw new SerializationException(
                    $"Can't convert {reader.TokenType} token to {nameof(BlittableJsonReaderObject)}. The value must to be a complex object");
            }

            return RecordingJson.ReadObject(ref reader);
        }
    }
}
