using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Sparrow.Json.Parsing
{
    public static class UnmanagedJsonParserHelper
    {
        public static unsafe string ReadString(JsonOperationContext context, PeepingTomStream peepingTomStream, UnmanagedJsonParser parser, JsonParserState state, JsonOperationContext.MemoryBuffer buffer)
        {
            if (Read(peepingTomStream, parser, state, buffer) == false)
                ThrowInvalidJson(peepingTomStream);

            if (state.CurrentTokenType == JsonParserToken.Null)
                return null;

            if (state.CurrentTokenType != JsonParserToken.String)
                ThrowInvalidJson(peepingTomStream);

            return context.AllocateStringValue(null, state.StringBuffer, state.StringSize).ToString();
        }

        public static bool Read(PeepingTomStream stream, UnmanagedJsonParser parser, JsonParserState state, JsonOperationContext.MemoryBuffer buffer)
        {
            while (parser.Read() == false)
            {
                var read = stream.Read(buffer.Memory.Memory.Span);
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

        public static async Task<bool> ReadAsync(PeepingTomStream peepingTomStream, UnmanagedJsonParser parser, JsonParserState state, JsonOperationContext.MemoryBuffer buffer, CancellationToken token = default)
        {
            if (parser.Read())
                return true;

            var read = await peepingTomStream.ReadAsync(buffer.Memory.Memory, token).ConfigureAwait(false);
            if (read == 0)
            {
                if (state.CurrentTokenType != JsonParserToken.EndObject)
                    throw new EndOfStreamException("Stream ended without reaching end of json content");

                return false;
            }

            parser.SetBuffer(buffer, 0, read);
            return parser.Read();
        }

        public static void ThrowInvalidJson(string msg, PeepingTomStream peepingTomStream, UnmanagedJsonParser parser)
        {
            string peepedWindow = GetPeepingTomBufferAsString(peepingTomStream);
            throw new InvalidOperationException("Invalid JSON. " + msg + " on " + parser.GenerateErrorState() + ". Last 4KB of read strem : '" + peepedWindow + "'");
        }

        public static void ThrowInvalidJson(PeepingTomStream peepingTomStream)
        {
            string peepedWindow = GetPeepingTomBufferAsString(peepingTomStream);
            throw new InvalidOperationException("Invalid JSON. Last 4KB of read stream : '" + peepedWindow + "'");
        }

        public static void ThrowInvalidJsonResponse(PeepingTomStream peepingTomStream)
        {
            string peepedWindow = GetPeepingTomBufferAsString(peepingTomStream);
            throw new InvalidDataException("Response is invalid JSONLast 4KB of read stream : '" + peepedWindow + "'");
        }

        private static string GetPeepingTomBufferAsString(PeepingTomStream peepingTomStream)
        {
            string peepedWindow;
            try
            {
                peepedWindow = Encodings.Utf8.GetString(peepingTomStream.PeepInReadStream());
            }
            catch (Exception e)
            {
                peepedWindow = "Failed to generated peepedWindow: " + e;
            }
            return peepedWindow;
        }

        public static void ReadObject(BlittableJsonDocumentBuilder builder, PeepingTomStream peepingTomStream, UnmanagedJsonParser parser, JsonOperationContext.MemoryBuffer buffer)
        {
            builder.ReadNestedObject();
            while (builder.Read() == false)
            {
                var read = peepingTomStream.Read(buffer.Memory.Memory.Span);
                if (read == 0)
                    throw new EndOfStreamException("Stream ended without reaching end of json content" + GetPeepingTomBufferAsString(peepingTomStream));

                parser.SetBuffer(buffer, 0, read);
            }
            builder.FinalizeDocument();
        }

        public static void ReadProperty(BlittableJsonDocumentBuilder builder, PeepingTomStream peepingTomStream, UnmanagedJsonParser parser, JsonOperationContext.MemoryBuffer buffer)
        {
            builder.ReadProperty();
            while (builder.Read() == false)
            {
                var read = peepingTomStream.Read(buffer.Memory.Memory.Span);
                if (read == 0)
                    throw new EndOfStreamException("Stream ended without reaching end of json content" + GetPeepingTomBufferAsString(peepingTomStream));

                parser.SetBuffer(buffer, 0, read);
            }
            builder.FinalizeDocument();
        }

        public static async Task ReadPropertyAsync(BlittableJsonDocumentBuilder builder, PeepingTomStream peepingTomStream, UnmanagedJsonParser parser, JsonOperationContext.MemoryBuffer buffer, CancellationToken token = default)
        {
            builder.ReadProperty();
            while (builder.Read() == false)
            {
                var read = await peepingTomStream.ReadAsync(buffer.Memory.Memory, token).ConfigureAwait(false);
                if (read == 0)
                    throw new EndOfStreamException("Stream ended without reaching end of json content" + GetPeepingTomBufferAsString(peepingTomStream));

                parser.SetBuffer(buffer, 0, read);
            }
            builder.FinalizeDocument();
        }

        public static long ReadLong(JsonOperationContext context, PeepingTomStream peepingTomStream, UnmanagedJsonParser parser, JsonParserState state, JsonOperationContext.MemoryBuffer buffer)
        {
            if (Read(peepingTomStream, parser, state, buffer) == false)
                ThrowInvalidJson(peepingTomStream);

            if (state.CurrentTokenType != JsonParserToken.Integer)
                ThrowInvalidJson(peepingTomStream);

            return state.Long;
        }

        public static async Task ReadObjectAsync(BlittableJsonDocumentBuilder builder, PeepingTomStream peepingTomStream, UnmanagedJsonParser parser, JsonOperationContext.MemoryBuffer buffer, CancellationToken token = default)
        {
            builder.ReadNestedObject();
            while (builder.Read() == false)
            {
                var read = await peepingTomStream.ReadAsync(buffer.Memory.Memory, token).ConfigureAwait(false);
                if (read == 0)
                    throw new EndOfStreamException("Stream ended without reaching end of json content");

                parser.SetBuffer(buffer, 0, read);
            }
            builder.FinalizeDocument();
        }

        public static IEnumerable<BlittableJsonReaderObject> ReadArrayToMemory(JsonOperationContext context, PeepingTomStream peepingTomStream, UnmanagedJsonParser parser, JsonParserState state, JsonOperationContext.MemoryBuffer buffer)
        {
            if (Read(peepingTomStream, parser, state, buffer) == false)
                ThrowInvalidJson(peepingTomStream);

            if (state.CurrentTokenType != JsonParserToken.StartArray)
                ThrowInvalidJson(peepingTomStream);

            int docsCountOnCachedRenewSession = 0;
            bool cachedItemsRenew = false;
            while (true)
            {
                if (docsCountOnCachedRenewSession <= 16 * 1024)
                {
                    if (cachedItemsRenew)
                    {
                        context.CachedProperties.ClearRenew();
                        ++docsCountOnCachedRenewSession;
                    }
                }
                else
                {
                    context.Renew();
                    docsCountOnCachedRenewSession = 0;
                }

                if (Read(peepingTomStream, parser, state, buffer) == false)
                    ThrowInvalidJson(peepingTomStream);

                if (state.CurrentTokenType == JsonParserToken.EndArray)
                    break;

                using (var builder = new BlittableJsonDocumentBuilder(context, BlittableJsonDocumentBuilder.UsageMode.None, "readArray/singleResult", parser, state))
                {
                    cachedItemsRenew = builder.NeedClearPropertiesCache();
                    ReadObject(builder, peepingTomStream, parser, buffer);

                    yield return builder.CreateReader();
                }
            }
        }

        public static async IAsyncEnumerable<BlittableJsonReaderObject> ReadArrayToMemoryAsync(JsonOperationContext context, PeepingTomStream peepingTomStream, UnmanagedJsonParser parser, JsonParserState state, JsonOperationContext.MemoryBuffer buffer)
        {
            if (await ReadAsync(peepingTomStream, parser, state, buffer).ConfigureAwait(false) == false)
                ThrowInvalidJson(peepingTomStream);

            if (state.CurrentTokenType != JsonParserToken.StartArray)
                ThrowInvalidJson(peepingTomStream);

            int docsCountOnCachedRenewSession = 0;
            bool cachedItemsRenew = false;
            while (true)
            {
                if (docsCountOnCachedRenewSession <= 16 * 1024)
                {
                    if (cachedItemsRenew)
                    {
                        context.CachedProperties = new CachedProperties(context);
                        ++docsCountOnCachedRenewSession;
                    }
                }
                else
                {
                    context.Renew();
                    docsCountOnCachedRenewSession = 0;
                }

                if (await ReadAsync(peepingTomStream, parser, state, buffer).ConfigureAwait(false) == false)
                    ThrowInvalidJson(peepingTomStream);

                if (state.CurrentTokenType == JsonParserToken.EndArray)
                    break;

                using (var builder = new BlittableJsonDocumentBuilder(context, BlittableJsonDocumentBuilder.UsageMode.None, "readArray/singleResult", parser, state))
                {
                    cachedItemsRenew = builder.NeedClearPropertiesCache();
                    await ReadObjectAsync(builder, peepingTomStream, parser, buffer).ConfigureAwait(false);

                    yield return builder.CreateReader();
                }
            }
        }
    }
}
