using System;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Client.Documents.Indexes;

namespace Raven.Client.Json.Converters
{
    internal sealed class JsonObjectConverter : RavenJsonConverter
    {
        public static readonly JsonObjectConverter Instance = new JsonObjectConverter();

        private JsonObjectConverter()
        {
        }

        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
            throw new NotSupportedException($"We only support deserialization to '{nameof(JsonObject)}' or '{nameof(JsonObject.Metadata)}'.");
        }

        public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
        {
            var value = serializer.Deserialize<JObject>(reader);
            if (objectType == typeof(JsonObject))
                return new JsonObject(value);

            if (objectType == typeof(JsonObject.Metadata))
                return new JsonObject.Metadata(value);

            throw new NotSupportedException($"We only support deserialization to '{nameof(JsonObject)}' or '{nameof(JsonObject.Metadata)}'.");
        }

        public override bool CanConvert(Type objectType)
        {
            return objectType == typeof(JsonObject) || objectType == typeof(JsonObject.Metadata);
        }
    }
}
