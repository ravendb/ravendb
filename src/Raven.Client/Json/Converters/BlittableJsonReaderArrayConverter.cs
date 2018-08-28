using System;
using System.Runtime.Serialization;
using Newtonsoft.Json;
using Sparrow.Json;

namespace Raven.Client.Json.Converters
{
    internal sealed class BlittableJsonReaderArrayConverter : RavenJsonConverter
    {
        public static readonly BlittableJsonReaderArrayConverter Instance = new BlittableJsonReaderArrayConverter();

        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
            if (!(value is BlittableJsonReaderArray readerArray))
            {
                throw new SerializationException($"Try to write {value.GetType()} which is not {nameof(BlittableJsonReaderArray)} value with {nameof(BlittableJsonReaderArrayConverter)}");
            }

            writer.WriteStartArray();
            foreach (var item in readerArray)
            {
                serializer.Serialize(writer, item); 
            }
            writer.WriteEndArray();
        }

        public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
        {
            if (!(reader is BlittableJsonReader blittableReader))
            {
                throw new SerializationException(
                    $"Try to read {nameof(BlittableJsonReaderArray)} property/field by {reader.GetType()} which is unsuitable reader. Should use {nameof(BlittableJsonReader)}");
            }

            if (blittableReader.Value == null)
            {
                return null;
            }

            if (blittableReader.Value is BlittableJsonReaderArray readerArray)
            {
                //Skip in order to prevent unnecessary movement inside the blittable array
                blittableReader.SkipBlittableArrayInside();

                return readerArray;
            }
            throw new SerializationException(
                $"Can't convert {blittableReader.Value.GetType()} type to {objectType}. The value must to be an array");
        }

        public override bool CanConvert(Type objectType)
        {
            return objectType == typeof(BlittableJsonReaderArray);
        }
    }
}
