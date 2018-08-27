using System;
using System.Runtime.Serialization;
using Newtonsoft.Json;
using Sparrow.Json;

namespace Raven.Client.Json.Converters
{
    internal sealed class LazyStringValueJsonConverter : RavenJsonConverter
    {
        public static readonly LazyStringValueJsonConverter Instance = new LazyStringValueJsonConverter();

        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
            writer.WriteValue(value);
        }

        public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
        {
            if (!(reader is BlittableJsonReader blittableReader))
            {
                throw new SerializationException(
                    $"Try to read {nameof(LazyStringValue)} property/field by {reader.GetType()} which is unsuitable reader. Should use {nameof(BlittableJsonReader)}");
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
            throw new SerializationException($"Try to read {nameof(LazyStringValue)} from {blittableReader.Value.GetType()}. Should be string here");
        }

        public override bool CanConvert(Type objectType)
        {
            return objectType == typeof(LazyStringValue);
        }
    }
}
