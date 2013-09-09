using System;
using System.IO;
using Raven.Imports.Newtonsoft.Json;

namespace Raven.Abstractions.Json
{
    public class StreamFromJsonConverter: JsonConverter
    {
        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
            throw new NotSupportedException();
        }

        public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
        {
            string value = (string)reader.Value;
            return new MemoryStream(Convert.FromBase64String(value));
        }

        public override bool CanConvert(Type objectType)
        {
            return objectType == typeof (Stream);
        }
    }
}