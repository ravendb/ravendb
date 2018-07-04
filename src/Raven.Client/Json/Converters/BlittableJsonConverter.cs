using System;
using System.Collections.Generic;
using System.Text;
using Newtonsoft.Json;
using Sparrow.Json;

namespace Raven.Client.Json.Converters
{

    internal sealed class BlittableJsonConverter : RavenJsonConverter
    {
        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
            writer.WriteValue(value);
        }

        public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
        {
            return null;
        }

        public override bool CanConvert(Type objectType)
        {
            return objectType == typeof(BlittableJsonReaderObject);
        }
    }
}
