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
                throw new SerializationException(
                    $"Try to write {nameof(LazyStringValue)} property/field by {writer.GetType()} which is unsuitable reader. Should use {nameof(BlittableJsonWriter)}");
            }

            if (!(value is Stream stream))
            {
                throw new SerializationException($"Try to write {value.GetType()} which is not {nameof(Stream)} value with {nameof(StreamConverter)}");
            }

            if (false == stream.CanSeek)
            {
                throw new SerializationException("Try to serialize stream that can't seek");
            }

            stream.Seek(0, SeekOrigin.Begin);
            var buffer = stream.ReadData();
            stream.Seek(0, SeekOrigin.Begin);
            var strBuffer = Convert.ToBase64String(buffer);
            blittableJsonWriter.WriteValue(strBuffer);
        }

        public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
        {
            if (!(reader is BlittableJsonReader blittableReader))
            {
                throw new SerializationException(
                    $"Try to read {nameof(Stream)} property/field by {reader.GetType()} which is unsuitable reader. Should use {nameof(BlittableJsonReader)}");
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
            throw new SerializationException($"Try to read {nameof(Stream)} from {blittableReader.Value.GetType()}. Should be string here");
        }

        public override bool CanConvert(Type objectType)
        {
            return typeof(Stream).IsAssignableFrom(objectType);
        }
    }
}
