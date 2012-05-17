using System;
using Newtonsoft.Json;
using Raven.Json.Linq;

namespace Raven.Abstractions.Smuggler
{
    public class TrivialJsonToJsonJsonConverter : JsonConverter
    {
        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
            throw new NotImplementedException();
        }

        public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
        {
            return RavenJObject.Load(reader);
        }

        public override bool CanConvert(Type objectType)
        {
            return objectType == typeof(RavenJObject);
        }
    }
}