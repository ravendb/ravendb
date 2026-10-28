using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Raven.Client.Json.Serialization.SystemTextJson.Internal.Converters
{
    internal sealed class ExpandoObjectConverter : JsonConverter<ExpandoObject>
    {
        public static readonly ExpandoObjectConverter Instance = new();

        private ExpandoObjectConverter()
        {
        }

        public override ExpandoObject Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            if (reader.TokenType != JsonTokenType.StartObject)
                throw new JsonException($"Expected StartObject when reading {nameof(ExpandoObject)}, got {reader.TokenType}.");

            return ReadObject(ref reader);
        }

        public override void Write(Utf8JsonWriter writer, ExpandoObject value, JsonSerializerOptions options)
        {
            writer.WriteStartObject();
            foreach (KeyValuePair<string, object> entry in value)
            {
                writer.WritePropertyName(options.DictionaryKeyPolicy?.ConvertName(entry.Key) ?? entry.Key);
                JsonSerializer.Serialize(writer, entry.Value, typeof(object), options);
            }

            writer.WriteEndObject();
        }

        private static ExpandoObject ReadObject(ref Utf8JsonReader reader)
        {
            var expando = new ExpandoObject();
            IDictionary<string, object> values = expando;
            while (reader.Read() && reader.TokenType != JsonTokenType.EndObject)
            {
                string name = reader.GetString();
                reader.Read();
                values[name] = ReadValue(ref reader);
            }

            return expando;
        }

        private static List<object> ReadList(ref Utf8JsonReader reader)
        {
            var list = new List<object>();
            while (reader.Read() && reader.TokenType != JsonTokenType.EndArray)
                list.Add(ReadValue(ref reader));

            return list;
        }

        private static object ReadValue(ref Utf8JsonReader reader)
        {
            switch (reader.TokenType)
            {
                case JsonTokenType.StartObject:
                    return ReadObject(ref reader);
                case JsonTokenType.StartArray:
                    return ReadList(ref reader);
                default:
                    return JsonDynamicConverters.ReadPrimitive(ref reader);
            }
        }
    }
}
