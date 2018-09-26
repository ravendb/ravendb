using System;
using System.IO;
using System.Runtime.Serialization;
using Newtonsoft.Json;
using Raven.Client.Extensions.Streams;
using Raven.Client.Json;

namespace Raven.Server.Json.Converters
{
    internal sealed class StreamConverter : RavenTypeJsonConverter<Stream>
    {
        public static readonly StreamConverter Instance = new StreamConverter();

        private StreamConverter() {}

        protected override void WriteJson(BlittableJsonWriter writer, Stream value, JsonSerializer serializer)
        {
            if (false == value.CanSeek)
            {
                throw new SerializationException("Try to serialize stream that can't seek");
            }

            value.Seek(0, SeekOrigin.Begin);
            var buffer = value.ReadData();
            value.Seek(0, SeekOrigin.Begin);
            var strBuffer = Convert.ToBase64String(buffer);
            writer.WriteValue(strBuffer);
        }

        internal override Stream ReadJson(BlittableJsonReader blittableReader)
        {
            if (!(blittableReader.Value is string strValue))
            {
                throw new SerializationException($"Try to read {nameof(Stream)} from {blittableReader.Value?.GetType()}. Should be string here");
            }

            var buffer = Convert.FromBase64String(strValue);
            return new MemoryStream(buffer);
        }
    }
}
