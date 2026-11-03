using System;
using Newtonsoft.Json;
using Raven.Client.Documents.Session;

namespace Raven.Client.Json.Serialization.NewtonsoftJson.Internal.Converters
{
    internal sealed class JsonIMetadataDictionaryConverter : JsonConverter
    {
        public static readonly JsonIMetadataDictionaryConverter Instance = new JsonIMetadataDictionaryConverter();

        private JsonIMetadataDictionaryConverter()
        {
        }

        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
        }

        public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
        {
            return serializer.Deserialize(reader, typeof(MetadataAsDictionary));
        }

        public override bool CanConvert(Type objectType)
        {
            return objectType == typeof(IMetadataDictionary);
        }
    }
}
