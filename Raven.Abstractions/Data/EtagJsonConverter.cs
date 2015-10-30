using System;
using Raven.Imports.Newtonsoft.Json;

namespace Raven.Abstractions.Data
{
    public class EtagJsonConverter : JsonConverter
    {
        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
            var etag = value as Etag;
            if (etag == null)
                writer.WriteNull();
            else
                writer.WriteValue(etag.ToString());
        }

        public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
        {
            var s = reader.Value as string;
            if (s == null)
                return null;
            return Etag.Parse(s);
        }

        public override bool CanConvert(Type objectType)
        {
            return objectType == typeof(Etag);
        }
    }
}
