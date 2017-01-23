using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Raven.Server.Documents.Patch;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers
{
    public class BatchRequestParser
    {
        public enum CommandType
        {
            None,
            PUT,
            PATCH,
            DELETE
        }

        public class CommandData
        {
            public CommandType Method;
            // TODO: Change to ID
            public string Key;
            public BlittableJsonReaderObject Document;
            public PatchRequest Patch;
            public PatchRequest PatchIfMissing;
            public long? Etag;
        }

        public static async Task<List<CommandData>> ParseAsync(JsonOperationContext ctx, Stream stream)
        {
            var results = new List<CommandData>();
            var state = new JsonParserState();
            JsonOperationContext.ManagedPinnedBuffer buffer;
            using (ctx.GetManagedBuffer(out buffer))
            using (var parser = new UnmanagedJsonParser(ctx, state, "bulk_docs"))
            {
                while (parser.Read() == false)
                    await RefillParserBuffer(stream, buffer, parser);

                if (state.CurrentTokenType != JsonParserToken.StartArray)
                {
                    ThrowUnexpectedToken(JsonParserToken.StartArray, state);
                }

                while (true)
                {
                    while (parser.Read() == false)
                        await RefillParserBuffer(stream, buffer, parser);

                    if (state.CurrentTokenType == JsonParserToken.EndArray)
                        break;

                    if (state.CurrentTokenType != JsonParserToken.StartObject)
                    {
                        ThrowUnexpectedToken(JsonParserToken.StartObject, state);
                    }

                    var cmd = new CommandData();
                    results.Add(cmd);

                    while (state.CurrentTokenType != JsonParserToken.EndObject)
                    {
                        while (parser.Read() == false)
                            await RefillParserBuffer(stream, buffer, parser);
                        if (state.CurrentTokenType != JsonParserToken.String)
                        {
                            ThrowUnexpectedToken(JsonParserToken.String, state);
                        }
                        switch (GetPropertyType(state, ctx))
                        {
                            case CommandPropertyName.Method:
                                while (parser.Read() == false)
                                    await RefillParserBuffer(stream, buffer, parser);
                                if (state.CurrentTokenType != JsonParserToken.String)
                                {
                                    ThrowUnexpectedToken(JsonParserToken.String, state);
                                }
                                cmd.Method = GetMethodType(state, ctx);
                                break;
                            case CommandPropertyName.Key:
                                while (parser.Read() == false)
                                    await RefillParserBuffer(stream, buffer, parser);
                                cmd.Key = GetDocumentKey(state);
                                break;
                            case CommandPropertyName.Document:
                                while (parser.Read() == false)
                                    await RefillParserBuffer(stream, buffer, parser);
                                cmd.Document = await ReadJsonObject(ctx, stream, cmd, parser, state, buffer);
                                break;
                            case CommandPropertyName.Patch:
                                var patch = await ReadJsonObject(ctx, stream, cmd, parser, state, buffer);
                                cmd.Patch = PatchRequest.Parse(patch);
                                break;

                            case CommandPropertyName.PatchIfMissing:
                                var patchIfMissing = await ReadJsonObject(ctx, stream, cmd, parser, state, buffer);
                                cmd.PatchIfMissing = PatchRequest.Parse(patchIfMissing);
                                break;
                            case CommandPropertyName.Etag:
                                while (parser.Read() == false)
                                    await RefillParserBuffer(stream, buffer, parser);
                                if (state.CurrentTokenType != JsonParserToken.Integer)
                                {
                                    ThrowUnexpectedToken(JsonParserToken.Integer, state);
                                }
                                cmd.Etag = state.Long;
                                break;
                        }
                    }

                    switch (cmd.Method)
                    {
                        case CommandType.None:
                            ThrowInvalidMethod();
                            break;
                        case CommandType.PUT:
                            if(cmd.Document==null)
                                ThrowMissingDocumentProperty();
                            break;
                        case CommandType.PATCH:
                            if (cmd.Patch == null)
                                ThrowMissingPatchProperty();
                            break;
                    }

                    while (parser.Read() == false)
                        await RefillParserBuffer(stream, buffer, parser);
                    if (state.CurrentTokenType != JsonParserToken.EndObject)
                    {
                        ThrowUnexpectedToken(JsonParserToken.EndObject, state);
                    }
                }
            }
            return results;
        }

        private static void ThrowInvalidMethod()
        {
            throw new InvalidOperationException("Command must have a valid 'Method' property");
        }

        private static void ThrowMissingDocumentProperty()
        {
            throw new InvalidOperationException("PUT command must have a 'Document' property");
        }

        private static void ThrowMissingPatchProperty()
        {
            throw new InvalidOperationException("PUT command must have a 'Patch' property");
        }

        private static async Task<BlittableJsonReaderObject> ReadJsonObject(JsonOperationContext ctx, Stream stream, CommandData cmd, UnmanagedJsonParser parser,
            JsonParserState state, JsonOperationContext.ManagedPinnedBuffer buffer)
        {
            BlittableJsonReaderObject reader;


            using (var builder = new BlittableJsonDocumentBuilder(ctx,
                BlittableJsonDocumentBuilder.UsageMode.ToDisk,
                cmd.Key, parser, state))
            {
                ctx.CachedProperties.NewDocument();
                builder.ReadNestedObject();
                while (true)
                {
                    if (builder.Read())
                        break;
                    await RefillParserBuffer(stream, buffer, parser);
                }
                builder.FinalizeDocument();
                reader = builder.CreateReader();
                ctx.RegisterLiveReader(reader);
                reader.NoCache = true;
            }
            return reader;
        }

        private static unsafe string GetDocumentKey(JsonParserState state)
        {
            return Encoding.UTF8.GetString(state.StringBuffer, state.StringSize);
        }

        private enum CommandPropertyName
        {
            NoSuchProperty,
            Method,
            Key,
            Document,
            Etag,
            Patch,
            PatchIfMissing
        }

        private static unsafe CommandPropertyName GetPropertyType(JsonParserState state, JsonOperationContext ctx)
        {
            // here we confirm that the value is matching our expectation, in order to save CPU instructions
            // we compare directly against the precomputed values
            switch (state.StringSize)
            {
                case 6:

                    if (*(int*)state.StringBuffer != 1752458573 ||
                       *(short*)(state.StringBuffer + 4) != 25711)
                        ThrowInvalidProperty(state, ctx);

                    return CommandPropertyName.Method;

                case 3:
                    if (*(short*)state.StringBuffer != 25931 ||
                        state.StringBuffer[2] != (byte)'y')
                        ThrowInvalidProperty(state, ctx);

                    return CommandPropertyName.Key;

                case 8:
                    if (*(long*)state.StringBuffer != 8389754676633104196)
                        ThrowInvalidProperty(state, ctx);

                    return CommandPropertyName.Document;

                case 4:
                    if (*(int*)state.StringBuffer == 1734440005)
                        ThrowInvalidProperty(state, ctx);

                    return CommandPropertyName.Etag;

                case 5:
                    if (*(int*)state.StringBuffer != 1668571472 ||
                       (state.StringBuffer[4]) != (byte)'h')
                        ThrowInvalidProperty(state, ctx);

                    return CommandPropertyName.Patch;

                case 14:

                    if (*(long*)state.StringBuffer != 5577225901238935888 ||
                       *(int*)(state.StringBuffer + 8) != 1769173865 ||
                       *(short*)(state.StringBuffer + 12) != 26478)
                        ThrowInvalidProperty(state, ctx);

                    return CommandPropertyName.PatchIfMissing;


                default:
                    ThrowInvalidProperty(state, ctx);
                    return CommandPropertyName.NoSuchProperty;
            }
        }

        private static unsafe CommandType GetMethodType(JsonParserState state, JsonOperationContext ctx)
        {
            // here we confirm that the value is matching our expectation, in order to save CPU instructions
            // we compare directly against the precomputed values
            switch (state.StringSize)
            {
                case 3:
                    if (*(short*)state.StringBuffer != 21840 ||
                        state.StringBuffer[2] != (byte)'T')
                        ThrowInvalidProperty(state, ctx);

                    return CommandType.PUT;

                case 5:
                    if (*(int*)state.StringBuffer != 1129595216 ||
                        state.StringBuffer[4] != (byte)'H')
                        ThrowInvalidProperty(state, ctx);

                    return CommandType.PATCH;

                case 6:
                    if (*(int*)state.StringBuffer != 1162626372 ||
                     *(short*)(state.StringBuffer + 4) != 17748)
                        ThrowInvalidProperty(state, ctx);

                    return CommandType.DELETE;

                default:
                    ThrowInvalidProperty(state, ctx);
                    return CommandType.None;
            }
        }

        private static unsafe void ThrowInvalidProperty(JsonParserState state, JsonOperationContext ctx)
        {
            throw new InvalidOperationException("Invalid property name: " +
                                                new LazyStringValue(null, state.StringBuffer, state.StringSize, ctx));
        }

        private static void ThrowUnexpectedToken(JsonParserToken jsonParserToken, JsonParserState state)
        {
            throw new InvalidOperationException("Expected " + jsonParserToken + " , but got " + state.CurrentTokenType);
        }

        private static async Task RefillParserBuffer(Stream stream, JsonOperationContext.ManagedPinnedBuffer buffer, UnmanagedJsonParser parser)
        {
            var read = await stream.ReadAsync(buffer.Buffer.Array, buffer.Buffer.Offset, buffer.Buffer.Count);
            if (read == 0)
                ThrowUnexpectedEndOfStream();
            parser.SetBuffer(buffer, read);
        }

        private static void ThrowUnexpectedEndOfStream()
        {
            throw new EndOfStreamException();
        }

    }
}