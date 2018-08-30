using System;
using System.Runtime.Serialization;
using Newtonsoft.Json;
using Sparrow.Json;

namespace Raven.Client.Json.Converters
{
    internal sealed class BlittableJsonReaderArrayConverter : RavenTypeJsonConverter<BlittableJsonReaderArray>
    {
        public static readonly BlittableJsonReaderArrayConverter Instance = new BlittableJsonReaderArrayConverter();

        private BlittableJsonReaderArrayConverter() {}

        protected override void WriteJson(BlittableJsonWriter writer, BlittableJsonReaderArray value, JsonSerializer serializer)
        {
            writer.WriteStartArray();
            foreach (var item in value)
            {
                serializer.Serialize(writer, item);
            }
            writer.WriteEndArray();
        }

        internal override BlittableJsonReaderArray ReadJson(BlittableJsonReader blittableReader)
        {
            if (!(blittableReader.Value is BlittableJsonReaderArray blittableArrayValue))
            {
                throw new SerializationException(
                    $"Can't convert {blittableReader.Value.GetType()} type to {nameof(BlittableJsonReaderArray)}. The value must to be an array");
            }
            
            //Skip in order to prevent unnecessary movement inside the blittable array
            blittableReader.SkipBlittableArrayInside();

            return
                blittableArrayValue.BelongsToContext(blittableReader.Context)
                    ? blittableArrayValue
                    : blittableArrayValue.Clone(blittableReader.Context);
        }
    }
}
