using System;
using System.Runtime.Serialization;
using Newtonsoft.Json;
using Sparrow.Json;

namespace Raven.Client.Json.Converters
{

    internal sealed class BlittableJsonConverter : RavenJsonConverter
    {
        public static readonly BlittableJsonConverter Instance = new BlittableJsonConverter();

        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
            writer.WriteValue(value);
        }

        public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
        {
            if (!(reader is BlittableJsonReader blittableReader))
            {
                throw new SerializationException(
                    $"Try to read {nameof(BlittableJsonReaderObject)} property/field by {reader.GetType()} witch is unsuitable reader. Should use {nameof(BlittableJsonReader)}");
            }

            if (blittableReader.Value == null)
            {
                return null;
            }

            if (blittableReader.Value is BlittableJsonReaderObject blittableValue)
            {
                if (reader.TokenType == JsonToken.StartObject)
                {
                    //Skip in order to prevent unnecessary movement inside the blittable
                    //when field\property type is BlittableJsonReaderObject
                    blittableReader.SkipBlittableInside();
                }

                return
                    blittableValue.BelongsToContext(blittableReader.Context) &&
                    blittableValue.HasParent == false
                        ? blittableValue
                        : blittableValue.Clone(blittableReader.Context);
            }
            throw new SerializationException(
                $"Can't convert {blittableReader.Value.GetType()} type to {nameof(BlittableJsonReaderObject)}. The value must to be a complex object");
        }

        public override bool CanConvert(Type objectType)
        {
            return objectType == typeof(BlittableJsonReaderObject);
        }
    }
}
