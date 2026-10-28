using System;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Raven.Client.Json.Serialization.SystemTextJson.Internal.Converters
{
    internal static class JsonDynamicConverters
    {
        public static readonly JsonConverter<JsonDynamicValue> Value = new JsonDynamicValueConverter();

        public static object ReadDocument(ref Utf8JsonReader reader)
        {
            switch (reader.TokenType)
            {
                case JsonTokenType.StartObject:
                    var dynamicObject = new JsonDynamicObject();
                    while (reader.Read() && reader.TokenType != JsonTokenType.EndObject)
                    {
                        string name = reader.GetString();
                        reader.Read();
                        dynamicObject[name] = ReadDocument(ref reader);
                    }

                    return dynamicObject;
                case JsonTokenType.StartArray:
                    var dynamicArray = new JsonDynamicArray();
                    while (reader.Read() && reader.TokenType != JsonTokenType.EndArray)
                        dynamicArray.Add(ReadDocument(ref reader));

                    return dynamicArray;
                default:
                    return new JsonDynamicValue(ReadPrimitive(ref reader));
            }
        }

        public static object ReadPrimitive(ref Utf8JsonReader reader)
        {
            switch (reader.TokenType)
            {
                case JsonTokenType.String:
                    return reader.GetString();
                case JsonTokenType.Number:
                    if (reader.TryGetInt64(out long longValue))
                        return longValue;

                    if (reader.TryGetDecimal(out decimal decimalValue))
                        return decimalValue;

                    return reader.GetDouble();
                case JsonTokenType.True:
                    return true;
                case JsonTokenType.False:
                    return false;
                case JsonTokenType.Null:
                    return null;
                default:
                    throw new JsonException($"Unexpected token '{reader.TokenType}' when reading a JSON value.");
            }
        }

        private sealed class JsonDynamicValueConverter : JsonConverter<JsonDynamicValue>
        {
            public override bool HandleNull => true;

            public override JsonDynamicValue Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options) =>
                new(ReadPrimitive(ref reader));

            public override void Write(Utf8JsonWriter writer, JsonDynamicValue value, JsonSerializerOptions options)
            {
                if (value?.Value == null)
                {
                    writer.WriteNullValue();
                    return;
                }

                JsonSerializer.Serialize(writer, value.Value, value.Value.GetType(), options);
            }
        }
    }
}
