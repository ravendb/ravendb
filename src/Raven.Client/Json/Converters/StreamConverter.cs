using System;
using System.IO;
using System.Reflection;
using System.Runtime.Serialization;
using Newtonsoft.Json;
using Raven.Client.Extensions.Streams;
using Sparrow.Json;

namespace Raven.Client.Json.Converters
{
    internal sealed class StreamConverter : RavenJsonConverter
    {
        public static readonly StreamConverter Instance = new StreamConverter();

        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
            if (!(writer is BlittableJsonWriter blittableJsonWriter))
            {
                throw new SerializationException($"{nameof(BlittableJsonWriter)} must to be used for serialize {nameof(Stream)}");
            }

            if (!(value is Stream stream))
            {
                throw new SerializationException($"Try to write non {nameof(Stream)} value with {nameof(StreamConverter)}");
            }

            //Todo To consider if Seek need to be here or should it come ready
            stream.Seek(0, SeekOrigin.Begin);
            var buffer = stream.ReadData();
            var strBuffer = Convert.ToBase64String(buffer);
            blittableJsonWriter.WriteValue(strBuffer);
        }

        public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
        {
            if (!(reader is BlittableJsonReader blittableReader))
            {
                throw new SerializationException($"{nameof(BlittableJsonReader)} must to be used for deserialize {nameof(Stream)}");
            }

            if (blittableReader.Value == null)
            {
                return null;
            }

            if (blittableReader.Value is string strValue)
            {
                var buffer = Convert.FromBase64String(strValue);
                return new MemoryStream(buffer);
            }
            throw new SerializationException($"Try to read {nameof(LazyStringValue)} from non {nameof(LazyStringValue)} value");
        }

        public override bool CanConvert(Type objectType)
        {
            return typeof(Stream).IsAssignableFrom(objectType);
        }
    }
}
