using System;
using System.Runtime.Serialization;
using Newtonsoft.Json;
using Sparrow.Json;

namespace Raven.Client.Json.Converters
{
    internal sealed class LazyStringValueJsonConverter : RavenJsonConverter
    {
        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
            writer.WriteValue(value);
        }

        public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
        {
            if (!(reader is BlittableJsonReader blittableReader))
            {
                throw new SerializationException($"{nameof(BlittableJsonReader)} must to be used for convert to {nameof(BlittableJsonReaderObject)}");
            }

            if (blittableReader.Value == null)
            {
                return null;
            }

            //Todo It will be better to change the reader to set the value as LazyStringValue 
            if (blittableReader.Value is string strValue)
            {
                return blittableReader.Context.GetLazyString(strValue);
            }
            throw new SerializationException($"Try to read {nameof(LazyStringValue)} from non {nameof(LazyStringValue)} value");
        }

        public override bool CanConvert(Type objectType)
        {
            return objectType == typeof(LazyStringValue);
        }
    }
}
