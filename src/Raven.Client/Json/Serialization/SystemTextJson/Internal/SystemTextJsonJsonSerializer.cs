using System;
using System.Text.Json;

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
            byte[] bytes = JsonSerializer.SerializeToUtf8Bytes(value, objectType, Options);
            WriteJsonToBlittableWriter(writer, bytes);
        }

        public object Deserialize(IJsonReader reader, Type type)
        {
            SystemTextJsonBlittableReader stjReader = (SystemTextJsonBlittableReader)reader;
            return JsonSerializer.Deserialize(stjReader.GetUtf8Json(), type, Options);
        }

        public T Deserialize<T>(IJsonReader reader)
        {
            SystemTextJsonBlittableReader stjReader = (SystemTextJsonBlittableReader)reader;
            return JsonSerializer.Deserialize<T>(stjReader.GetUtf8Json(), Options);
        }

        private static void WriteJsonToBlittableWriter(IJsonWriter writer, byte[] utf8Json)
        {
            Utf8JsonReader jsonReader = new Utf8JsonReader(utf8Json);
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
