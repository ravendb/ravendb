using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace Sparrow.Json.Parsing
{
    public static class UnmanagedJsonParserHelper
    {
        public static unsafe string ReadString(JsonOperationContext context, Stream stream, UnmanagedJsonParser parser, JsonParserState state, JsonOperationContext.ManagedPinnedBuffer buffer)
        {
            if (Read(stream, parser, state, buffer) == false)
                ThrowInvalidJson();

            if (state.CurrentTokenType != JsonParserToken.String)
                ThrowInvalidJson();

            return context.AllocateStringValue(null, state.StringBuffer, state.StringSize).ToString();
        }

        public static bool Read(Stream stream, UnmanagedJsonParser parser, JsonParserState state, JsonOperationContext.ManagedPinnedBuffer buffer)
        {
            if (parser.Read())
                return true;

            var read = stream.Read(buffer.Buffer.Array, buffer.Buffer.Offset, buffer.Length);
            if (read == 0)
            {
                if (state.CurrentTokenType != JsonParserToken.EndObject)
                    throw new EndOfStreamException("Stream ended without reaching end of json content");

                return false;
            }

            parser.SetBuffer(buffer, 0, read);
            return parser.Read();
        }

        public static async Task<bool> ReadAsync(Stream stream, UnmanagedJsonParser parser, JsonParserState state, JsonOperationContext.ManagedPinnedBuffer buffer)
        {
            if (parser.Read())
                return true;

            var read = await stream.ReadAsync(buffer.Buffer.Array, buffer.Buffer.Offset, buffer.Length).ConfigureAwait(false);
            if (read == 0)
            {
                if (state.CurrentTokenType != JsonParserToken.EndObject)
                    throw new EndOfStreamException("Stream ended without reaching end of json content");

                return false;
            }

            parser.SetBuffer(buffer, 0, read);
            return parser.Read();
        }

        public static void ThrowInvalidJson()
        {
            throw new InvalidOperationException("Invalid JSON.");
        }

        public static void ReadObject(BlittableJsonDocumentBuilder builder, Stream stream, UnmanagedJsonParser parser, JsonOperationContext.ManagedPinnedBuffer buffer)
        {
            builder.ReadNestedObject();
            while (builder.Read() == false)
            {
                var read = stream.Read(buffer.Buffer.Array, buffer.Buffer.Offset, buffer.Length);
                if (read == 0)
                    throw new EndOfStreamException("Stream ended without reaching end of json content");

                parser.SetBuffer(buffer, 0, read);
            }
            builder.FinalizeDocument();
        }

        public static async Task ReadObjectAsync(BlittableJsonDocumentBuilder builder, Stream stream, UnmanagedJsonParser parser, JsonOperationContext.ManagedPinnedBuffer buffer)
        {
            builder.ReadNestedObject();
            while (builder.Read() == false)
            {
                var read = await stream.ReadAsync(buffer.Buffer.Array, buffer.Buffer.Offset, buffer.Length).ConfigureAwait(false);
                if (read == 0)
                    throw new EndOfStreamException("Stream ended without reaching end of json content");

                parser.SetBuffer(buffer, 0, read);
            }
            builder.FinalizeDocument();
        }

        public static IEnumerable<BlittableJsonReaderObject> ReadArrayToMemory(JsonOperationContext context, Stream stream, UnmanagedJsonParser parser, JsonParserState state, JsonOperationContext.ManagedPinnedBuffer buffer)
        {
            if (Read(stream, parser, state, buffer) == false)
                ThrowInvalidJson();

            if (state.CurrentTokenType != JsonParserToken.StartArray)
                ThrowInvalidJson();

            while (true)
            {
                if (Read(stream, parser, state, buffer) == false)
                    ThrowInvalidJson();

                if (state.CurrentTokenType == JsonParserToken.EndArray)
                    break;

                using (var builder = new BlittableJsonDocumentBuilder(context, BlittableJsonDocumentBuilder.UsageMode.None, "readArray/singleResult", parser, state))
                {
                    ReadObject(builder, stream, parser, buffer);

                    yield return builder.CreateReader();
                }
            }
        }
    }
}