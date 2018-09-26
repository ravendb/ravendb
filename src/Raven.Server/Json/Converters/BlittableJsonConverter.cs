using System.Runtime.Serialization;
using Newtonsoft.Json;
using Raven.Client.Json;
using Sparrow.Json;

namespace Raven.Server.Json.Converters
{

    internal sealed class BlittableJsonConverter : RavenTypeJsonConverter<BlittableJsonReaderObject>
    {
        public static readonly BlittableJsonConverter Instance = new BlittableJsonConverter();

        private BlittableJsonConverter() {}

        protected override void WriteJson(BlittableJsonWriter writer, BlittableJsonReaderObject value, JsonSerializer serializer)
        {
            writer.WriteValue(value);
        }

        internal override BlittableJsonReaderObject ReadJson(BlittableJsonReader blittableReader)
        {
            if (!(blittableReader.Value is BlittableJsonReaderObject blittableValue))
            {
                throw new SerializationException(
                    $"Can't convert {blittableReader.Value?.GetType()} type to {nameof(BlittableJsonReaderObject)}. The value must to be a complex object");
            }

            if (blittableReader.TokenType == JsonToken.StartObject)
            {
                //Because the value that return is the blittable as a whole
                //we skip the reading inside this blittable
                blittableReader.SkipBlittableInside();
            }

            return
                blittableValue.BelongsToContext(blittableReader.Context) &&
                blittableValue.HasParent == false
                    ? blittableValue
                    : blittableValue.Clone(blittableReader.Context);
        }
    }
}
