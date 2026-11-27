using System;
using System.Runtime.Serialization;
using System.Text.Json;
using System.Text.Json.Serialization;
using Raven.Server.ServerWide.Context;
using Sparrow.Server;
using Voron;

namespace Raven.Server.Json.Converters
{
    internal sealed class SliceJsonConverter : JsonConverter<Slice>
    {
        public static readonly SliceJsonConverter Instance = new SliceJsonConverter();

        private SliceJsonConverter()
        {
        }

        public override bool HandleNull => true;

        public override void Write(Utf8JsonWriter writer, Slice value, JsonSerializerOptions options)
        {
            if (value.HasValue == false)
            {
                writer.WriteNullValue();
                return;
            }

            writer.WriteBase64StringValue(value.AsReadOnlySpan());
        }

        public override Slice Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            if (reader.TokenType == JsonTokenType.Null)
                return default;

            if (reader.TokenType != JsonTokenType.String)
                throw new SerializationException($"Try to read {nameof(Slice)} from {reader.TokenType} token. Should be string here");

            if (!(RecordingJson.Context is DocumentsOperationContext context))
                throw new SerializationException($"{nameof(DocumentsOperationContext)} must to be used for reading {nameof(Slice)}");

            byte[] buffer = reader.GetBytesFromBase64();
            Slice.From(context.Allocator, buffer, ByteStringType.Immutable, out var slice);

            return slice;
        }
    }
}
