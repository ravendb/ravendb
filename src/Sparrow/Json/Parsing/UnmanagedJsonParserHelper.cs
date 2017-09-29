using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;

namespace Sparrow.Json.Parsing
{
    

    public static class UnmanagedJsonParserHelper
    {
        public static unsafe string ReadString(JsonOperationContext context, PeepingTomStream peepingTomStream, UnmanagedJsonParser parser, JsonParserState state, JsonOperationContext.ManagedPinnedBuffer buffer)
        {
            if (Read(peepingTomStream, parser, state, buffer) == false)
                ThrowInvalidJson(peepingTomStream);

            if (state.CurrentTokenType != JsonParserToken.String)
                ThrowInvalidJson(peepingTomStream);

            return context.AllocateStringValue(null, state.StringBuffer, state.StringSize).ToString();
        }

        public static bool Read(PeepingTomStream stream, UnmanagedJsonParser parser, JsonParserState state, JsonOperationContext.ManagedPinnedBuffer buffer)
        {
            while (parser.Read() == false)
            {
                var read = stream.Read(buffer.Buffer.Array, buffer.Buffer.Offset, buffer.Length);
                if (read == 0)
                {
                    if (state.CurrentTokenType != JsonParserToken.EndObject)
                        throw new EndOfStreamException("Stream ended without reaching end of json content");

                    return false;
                }

                parser.SetBuffer(buffer, 0, read);
            }
            return true;
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

        public static void ThrowInvalidJson(PeepingTomStream peepingTomStream)
        {
            string s = GetPeepingTomBufferAsString(peepingTomStream);

            throw new InvalidOperationException("Invalid JSON: " + s);
        }

        private static string GetPeepingTomBufferAsString(PeepingTomStream peepingTomStream)
        {
            string s;
            try
            {
                s = Encodings.Utf8.GetString(peepingTomStream.PeepInReadStream());
            }
            catch (Exception e)
            {
                s = e.Message;
            }

            return s;
        }

        public static void ReadObject(BlittableJsonDocumentBuilder builder, PeepingTomStream stream, UnmanagedJsonParser parser, JsonOperationContext.ManagedPinnedBuffer buffer)
        {
            builder.ReadNestedObject();
            while (builder.Read() == false)
            {
                var read = stream.Read(buffer.Buffer.Array, buffer.Buffer.Offset, buffer.Length);
                if (read == 0)
                    throw new EndOfStreamException("Stream ended without reaching end of json content" + GetPeepingTomBufferAsString(stream));

                parser.SetBuffer(buffer, 0, read);
            }
            builder.FinalizeDocument();
        }

        public static long ReadLong(JsonOperationContext context, Stream stream, UnmanagedJsonParser parser, JsonParserState state, JsonOperationContext.ManagedPinnedBuffer buffer)
        {
            var peepingTomStream = new PeepingTomStream(stream);
            if (Read(peepingTomStream, parser, state, buffer) == false)
                ThrowInvalidJson(peepingTomStream);

            if (state.CurrentTokenType != JsonParserToken.Integer)
                ThrowInvalidJson(peepingTomStream);

            return state.Long;
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

        public static IEnumerable<BlittableJsonReaderObject> ReadArrayToMemory(JsonOperationContext context, PeepingTomStream peepingTomStream, UnmanagedJsonParser parser, JsonParserState state, JsonOperationContext.ManagedPinnedBuffer buffer)
        {
            if (Read(peepingTomStream, parser, state, buffer) == false)
                ThrowInvalidJson(peepingTomStream);

            if (state.CurrentTokenType != JsonParserToken.StartArray)
                ThrowInvalidJson(peepingTomStream);

            while (true)
            {
                if (Read(peepingTomStream, parser, state, buffer) == false)
                    ThrowInvalidJson(peepingTomStream);

                if (state.CurrentTokenType == JsonParserToken.EndArray)
                    break;

                using (var builder = new BlittableJsonDocumentBuilder(context, BlittableJsonDocumentBuilder.UsageMode.None, "readArray/singleResult", parser, state))
                {
                    ReadObject(builder, peepingTomStream, parser, buffer);

                    yield return builder.CreateReader();
                }
            }
        }
    }
}
