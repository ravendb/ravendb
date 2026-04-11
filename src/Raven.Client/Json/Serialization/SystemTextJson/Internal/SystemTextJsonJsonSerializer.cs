using System;
using System.IO;
using System.Text.Json;
using Sparrow;

namespace Raven.Client.Json.Serialization.SystemTextJson.Internal
{
    internal sealed class SystemTextJsonJsonSerializer : IJsonSerializer
    {
        public JsonSerializerOptions Options { get; }

        public SystemTextJsonJsonSerializer(JsonSerializerOptions options)
        {
            Options = options;
        }

        public void Serialize(IJsonWriter writer, object value)
        {
            Serialize(writer, value, value?.GetType() ?? typeof(object));
        }

        public void Serialize(IJsonWriter writer, object value, Type objectType)
        {
            using var stream = RecyclableMemoryStreamFactory.GetRecyclableStream();
            using (var utf8Writer = new Utf8JsonWriter((Stream)stream))
            {
                JsonSerializer.Serialize(utf8Writer, value, objectType, Options);
            }

            stream.TryGetBuffer(out var buffer);
            WriteJsonToBlittableWriter(writer, new ReadOnlySpan<byte>(buffer.Array, buffer.Offset, buffer.Count));
        }

        public object Deserialize(IJsonReader reader, Type type)
        {
            var stjReader = (SystemTextJsonBlittableReader)reader;
            return JsonSerializer.Deserialize(stjReader.GetUtf8Json(), type, Options);
        }

        public T Deserialize<T>(IJsonReader reader)
        {
            var stjReader = (SystemTextJsonBlittableReader)reader;
            return JsonSerializer.Deserialize<T>(stjReader.GetUtf8Json(), Options);
        }

        private static void WriteJsonToBlittableWriter(IJsonWriter writer, ReadOnlySpan<byte> utf8Json)
        {
            var jsonReader = new Utf8JsonReader(utf8Json);
            while (jsonReader.Read())
            {
                switch (jsonReader.TokenType)
                {
                    case JsonTokenType.StartObject:
                        writer.WriteStartObject();
                        break;
                    case JsonTokenType.EndObject:
                        writer.WriteEndObject();
                        break;
                    case JsonTokenType.StartArray:
                        writer.WriteStartArray();
                        break;
                    case JsonTokenType.EndArray:
                        writer.WriteEndArray();
                        break;
                    case JsonTokenType.PropertyName:
                        writer.WritePropertyName(jsonReader.GetString());
                        break;
                    case JsonTokenType.String:
                        writer.WriteValue(jsonReader.GetString());
                        break;
                    case JsonTokenType.Number:
                        if (jsonReader.TryGetInt64(out long l))
                            writer.WriteValue(l);
                        else
                            writer.WriteValue(jsonReader.GetDouble());
                        break;
                    case JsonTokenType.True:
                        writer.WriteValue(true);
                        break;
                    case JsonTokenType.False:
                        writer.WriteValue(false);
                        break;
                    case JsonTokenType.Null:
                        writer.WriteNull();
                        break;
                }
            }
        }
    }
}
