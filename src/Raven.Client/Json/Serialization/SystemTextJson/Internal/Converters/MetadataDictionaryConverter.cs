using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Serialization;
using Raven.Client.Documents.Session;
using Raven.Client.Json;

namespace Raven.Client.Json.Serialization.SystemTextJson.Internal.Converters
{
    internal sealed class MetadataDictionaryConverter : JsonConverter<IMetadataDictionary>
    {
        public static readonly MetadataDictionaryConverter Instance = new();

        private MetadataDictionaryConverter()
        {
        }

        public override IMetadataDictionary Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            if (reader.TokenType == JsonTokenType.Null)
                return null;

            if (reader.TokenType != JsonTokenType.StartObject)
                throw new JsonException($"Expected StartObject, got {reader.TokenType}");

            using (JsonDocument document = JsonDocument.ParseValue(ref reader))
            {
                return new MetadataAsDictionary((Dictionary<string, object>)ToClrValue(document.RootElement));
            }
        }

        public override void Write(Utf8JsonWriter writer, IMetadataDictionary value, JsonSerializerOptions options)
        {
            if (value == null)
            {
                writer.WriteNullValue();
                return;
            }

            SystemTextJsonSerializationConventions.WriteMetadata(writer, value);
        }

        private static object ToClrValue(JsonElement element)
        {
            switch (element.ValueKind)
            {
                case JsonValueKind.Object:
                    var dictionary = new Dictionary<string, object>();
                    foreach (JsonProperty property in element.EnumerateObject())
                        dictionary[property.Name] = ToClrValue(property.Value);
                    return dictionary;
                case JsonValueKind.Array:
                    var items = new object[element.GetArrayLength()];
                    int i = 0;
                    foreach (JsonElement item in element.EnumerateArray())
                        items[i++] = ToClrValue(item);
                    return items;
                case JsonValueKind.String:
                    return element.GetString();
                case JsonValueKind.Number:
                    return element.TryGetInt64(out long l) ? l : element.GetDouble();
                case JsonValueKind.True:
                    return true;
                case JsonValueKind.False:
                    return false;
                default:
                    return null;
            }
        }
    }
}
