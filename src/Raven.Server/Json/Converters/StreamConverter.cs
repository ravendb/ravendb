using System;
using System.IO;
using System.Runtime.Serialization;
using System.Text.Json;
using System.Text.Json.Serialization;
using Raven.Client.Extensions.Streams;

namespace Raven.Server.Json.Converters
{
    internal sealed class StreamConverter : JsonConverter<Stream>
    {
        public static readonly StreamConverter Instance = new StreamConverter();

        private StreamConverter()
        {
        }

        public override bool CanConvert(Type typeToConvert)
        {
            return typeof(Stream).IsAssignableFrom(typeToConvert);
        }

        public override void Write(Utf8JsonWriter writer, Stream value, JsonSerializerOptions options)
        {
            if (false == value.CanSeek)
            {
                throw new SerializationException("Try to serialize stream that can't seek");
            }

            value.Seek(0, SeekOrigin.Begin);
            var buffer = value.ReadData();
            value.Seek(0, SeekOrigin.Begin);
            writer.WriteBase64StringValue(buffer);
        }

        public override Stream Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            if (reader.TokenType != JsonTokenType.String)
                throw new SerializationException($"Try to read {nameof(Stream)} from {reader.TokenType} token. Should be string here");

            return new MemoryStream(reader.GetBytesFromBase64());
        }
    }
}
