using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
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
            if (!(reader is BlittableJsonReader blittableReader))
            {
                throw new SerializationException($"{nameof(BlittableJsonReader)} must to be used for convert to {nameof(BlittableJsonReaderObject)}");
            }

            if (blittableReader.Value == null)
            {
                return null;
            }

            if (blittableReader.Value is BlittableJsonReaderObject blittableValue)
            {
                //Skip in order to prevent unnecessary movement inside the blittable
                //when field\property type is BlittableJsonReaderObject
                blittableReader.SkipBlittableInside();

                return blittableValue.BelongsToContext(blittableReader.Context) 
                    ? blittableValue 
                    : blittableValue.Clone(blittableReader.Context);
            }
            throw new SerializationException($"Try to read {nameof(BlittableJsonReaderObject)} from non {nameof(BlittableJsonReaderObject)} value");

        }

        public override bool CanConvert(Type objectType)
        {
            return objectType == typeof(BlittableJsonReaderObject);
        }
    }
}
