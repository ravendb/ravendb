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
            Dictionary<string, object> dict = JsonSerializer.Deserialize<Dictionary<string, object>>(ref reader, options);
            return new MetadataAsDictionary(dict);
        }

        public override void Write(Utf8JsonWriter writer, IMetadataDictionary value, JsonSerializerOptions options)
        {
        }
    }
}
