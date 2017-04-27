using System;
using System.Collections.Generic;
using System.IO;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Extensions;
using Raven.Server.Documents.Patch;
using Raven.Server.Smuggler.Documents;
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

        public struct CommandData
        {
            public CommandType Method;
            // TODO: Change to ID
            public string Key;
            public BlittableJsonReaderObject Document;
            public PatchRequest Patch;
            public PatchRequest PatchIfMissing;
            public long? Etag;
            public bool KeyPrefixed;
        }

        [ThreadStatic]
        private static Stack<CommandData[]> _cache;

        private static readonly CommandData[] Empty = new CommandData[0];

        public static void ReturnBuffer(ArraySegment<CommandData> cmds)
        {
            Array.Clear(cmds.Array, cmds.Offset, cmds.Count);
            ReturnBuffer(cmds.Array);
        }

        private static void ReturnBuffer(CommandData[] cmds)
        {
            if (_cache == null)
                _cache = new Stack<CommandData[]>();

            if (_cache.Count > 1024)
                return;
            _cache.Push(cmds);
        }

        public static async Task<ArraySegment<CommandData>> ParseAsync(JsonOperationContext ctx, Stream stream)
        {
            CommandData[] cmds = Empty;

            int index = -1;
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

                    index++;
                    if (index >= cmds.Length)
                    {
                        cmds = IncreaseSizeOfCommandsBuffer(index, cmds);
                    }

                    cmds[index] = await ReadSingleCommand(ctx, stream, state, parser, buffer, default(CancellationToken));
                }
            }
            return new ArraySegment<CommandData>(cmds, 0, index + 1);
        }


        public class ReadMany : IDisposable
        {
            private readonly Stream _stream;
            private UnmanagedJsonParser _parser;
            private JsonOperationContext.ManagedPinnedBuffer _buffer;
            private JsonParserState _state;
            private CancellationToken _token;

            public ReadMany(JsonOperationContext ctx, Stream stream, JsonOperationContext.ManagedPinnedBuffer buffer, CancellationToken token)
            {
                _stream = stream;
                _buffer = buffer;
                _token = token;

                _state = new JsonParserState();
                _parser = new UnmanagedJsonParser(ctx, _state, "bulk_docs");
            }

            public async Task Init()
            {
                while (_parser.Read() == false)
                    await RefillParserBuffer(_stream, _buffer, _parser, _token);
                if (_state.CurrentTokenType != JsonParserToken.StartArray)
                {
                    ThrowUnexpectedToken(JsonParserToken.StartArray, _state);
                }
            }

            public void Dispose()
            {
                _parser.Dispose();
            }

            public Task<CommandData> MoveNext(JsonOperationContext ctx)
            {
                if (_parser.Read())
                {
                    if (_state.CurrentTokenType == JsonParserToken.EndArray)
                        return null;

                    return ReadSingleCommand(ctx, _stream, _state, _parser, _buffer, _token);
                }

                return MoveNextUnlikely(ctx);
            }

            private async Task<CommandData> MoveNextUnlikely(JsonOperationContext ctx)
            {
                do
                {
                    await RefillParserBuffer(_stream, _buffer, _parser, _token);

                } while (_parser.Read() == false);
                if (_state.CurrentTokenType == JsonParserToken.EndArray)
                    return new CommandData{Method = CommandType.None};

                return await ReadSingleCommand(ctx, _stream, _state, _parser, _buffer, _token);
            }
        }


        private static async Task<CommandData> ReadSingleCommand(
            JsonOperationContext ctx, 
            Stream stream, 
            JsonParserState state, 
            UnmanagedJsonParser parser, 
            JsonOperationContext.ManagedPinnedBuffer buffer,
            CancellationToken token)
        {
            var commandData = new CommandData();
            if (state.CurrentTokenType != JsonParserToken.StartObject)
            {
                ThrowUnexpectedToken(JsonParserToken.StartObject, state);
            }

            while (true)
            {
                while (parser.Read() == false)
                    await RefillParserBuffer(stream, buffer, parser, token);

                if (state.CurrentTokenType == JsonParserToken.EndObject)
                    break;

                if (state.CurrentTokenType != JsonParserToken.String)
                {
                    ThrowUnexpectedToken(JsonParserToken.String, state);
                }
                switch (GetPropertyType(state))
                {
                    case CommandPropertyName.Method:
                        while (parser.Read() == false)
                            await RefillParserBuffer(stream, buffer, parser, token);
                        if (state.CurrentTokenType != JsonParserToken.String)
                        {
                            ThrowUnexpectedToken(JsonParserToken.String, state);
                        }
                        commandData.Method = GetMethodType(state, ctx);
                        break;
                    case CommandPropertyName.Key:
                        while (parser.Read() == false)
                            await RefillParserBuffer(stream, buffer, parser, token);
                        switch (state.CurrentTokenType)
                        {
                            case JsonParserToken.Null:
                                commandData.Key = null;
                                break;
                            case JsonParserToken.String:
                                commandData.Key = GetDocumentKey(state);
                                break;
                            default:
                                ThrowUnexpectedToken(JsonParserToken.String, state);
                                break;
                        }
                        break;
                    case CommandPropertyName.Document:
                        while (parser.Read() == false)
                            await RefillParserBuffer(stream, buffer, parser, token);
                        commandData.Document = await ReadJsonObject(ctx, stream, commandData.Key, parser, state, buffer, token);
                        break;
                    case CommandPropertyName.Patch:
                        while (parser.Read() == false)
                            await RefillParserBuffer(stream, buffer, parser, token);
                        var patch = await ReadJsonObject(ctx, stream, commandData.Key, parser, state, buffer, token);
                        commandData.Patch = PatchRequest.Parse(patch);
                        break;
                    case CommandPropertyName.PatchIfMissing:
                        while (parser.Read() == false)
                            await RefillParserBuffer(stream, buffer, parser, token);
                        var patchIfMissing = await ReadJsonObject(ctx, stream, commandData.Key, parser, state, buffer, token);
                        commandData.PatchIfMissing = PatchRequest.Parse(patchIfMissing);
                        break;
                    case CommandPropertyName.Etag:
                        while (parser.Read() == false)
                            await RefillParserBuffer(stream, buffer, parser, token);
                        if (state.CurrentTokenType == JsonParserToken.Null)
                        {
                            commandData.Etag = null;
                        }
                        else
                        {
                            if (state.CurrentTokenType != JsonParserToken.Integer)
                            {
                                ThrowUnexpectedToken(JsonParserToken.Integer, state);
                            }

                            commandData.Etag = state.Long;
                        }
                        break;
                    case CommandPropertyName.KeyPrefixed:
                        while (parser.Read() == false)
                            await RefillParserBuffer(stream, buffer, parser, token);

                        if (state.CurrentTokenType != JsonParserToken.True && state.CurrentTokenType != JsonParserToken.False)
                        {
                            ThrowUnexpectedToken(JsonParserToken.True, state);
                        }

                        commandData.KeyPrefixed = state.CurrentTokenType == JsonParserToken.True;
                        break;
                    case CommandPropertyName.NoSuchProperty:
                        // unknown command - ignore it
                        while (parser.Read() == false)
                            await RefillParserBuffer(stream, buffer, parser, token);
                        if (state.CurrentTokenType == JsonParserToken.StartObject ||
                            state.CurrentTokenType == JsonParserToken.StartArray)
                        {
                            await ReadJsonObject(ctx, stream, commandData.Key, parser, state, buffer, token);
                        }
                        break;
                }
            }

            switch (commandData.Method)
            {
                case CommandType.None:
                    ThrowInvalidMethod();
                    break;
                case CommandType.PUT:
                    if (commandData.Document == null)
                        ThrowMissingDocumentProperty();
                    break;
                case CommandType.PATCH:
                    if (commandData.Patch == null)
                        ThrowMissingPatchProperty();
                    break;
            }

            return commandData;
        }

        private static CommandData[] IncreaseSizeOfCommandsBuffer(int index, CommandData[] cmds)
        {
            if (_cache == null)
                _cache = new Stack<CommandData[]>();
            CommandData[] tmp = null;
            while (_cache.Count > 0)
            {
                tmp = _cache.Pop();
                if (tmp.Length > index)
                    break;
                tmp = null;
            }
            if (tmp == null)
                tmp = new CommandData[cmds.Length + 8];
            Array.Copy(cmds, 0, tmp, 0, index);
            Array.Clear(cmds, 0, cmds.Length);
            ReturnBuffer(cmds);
            cmds = tmp;
            return cmds;
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

        private static async Task<BlittableJsonReaderObject> ReadJsonObject(JsonOperationContext ctx, Stream stream, string key, UnmanagedJsonParser parser,
            JsonParserState state, JsonOperationContext.ManagedPinnedBuffer buffer, CancellationToken token)
        {
            if (state.CurrentTokenType == JsonParserToken.Null)
                return null;

            BlittableJsonReaderObject reader;
            using (var builder = new BlittableJsonDocumentBuilder(ctx,
                BlittableJsonDocumentBuilder.UsageMode.ToDisk,
                key, parser, state, modifier: new StreamSource.BlittableMetadataModifier(ctx)))
            {
                ctx.CachedProperties.NewDocument();
                builder.ReadNestedObject();
                while (true)
                {
                    if (builder.Read())
                        break;
                    await RefillParserBuffer(stream, buffer, parser, token);
                }
                builder.FinalizeDocument();
                reader = builder.CreateReader();
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
            PatchIfMissing,
            KeyPrefixed
            // other properties are ignore (for legacy support)
        }

        private static unsafe CommandPropertyName GetPropertyType(JsonParserState state)
        {
            // here we confirm that the value is matching our expectation, in order to save CPU instructions
            // we compare directly against the precomputed values
            switch (state.StringSize)
            {
                case 6:

                    if (*(int*)state.StringBuffer != 1752458573 ||
                       *(short*)(state.StringBuffer + 4) != 25711)
                        // ThrowInvalidProperty(state, ctx);
                        return CommandPropertyName.NoSuchProperty;
                    return CommandPropertyName.Method;

                case 3:
                    if (*(short*)state.StringBuffer != 25931 ||
                        state.StringBuffer[2] != (byte)'y')
                        return CommandPropertyName.NoSuchProperty;
                    return CommandPropertyName.Key;

                case 8:
                    if (*(long*)state.StringBuffer != 8389754676633104196)
                        return CommandPropertyName.NoSuchProperty;
                    return CommandPropertyName.Document;

                case 4:
                    if (*(int*)state.StringBuffer != 1734440005)
                        return CommandPropertyName.NoSuchProperty;
                    return CommandPropertyName.Etag;

                case 5:
                    if (*(int*)state.StringBuffer != 1668571472 ||
                       (state.StringBuffer[4]) != (byte)'h')
                        return CommandPropertyName.NoSuchProperty;
                    return CommandPropertyName.Patch;

                case 14:
                    if (*(int*)state.StringBuffer != 1668571472 || 
                        *(long*)(state.StringBuffer + 4) != 7598543892411468136 || 
                        *(short*)(state.StringBuffer + 12) != 26478)
                        return CommandPropertyName.NoSuchProperty;
                    return CommandPropertyName.PatchIfMissing;

                case 11:
                    if (*(long*)state.StringBuffer != 7594869363257730379)
                        return CommandPropertyName.NoSuchProperty;

                    return CommandPropertyName.KeyPrefixed;

                default:
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
                                                ctx.AllocateStringValue(null, state.StringBuffer, state.StringSize));
        }

        private static void ThrowUnexpectedToken(JsonParserToken jsonParserToken, JsonParserState state)
        {
            throw new InvalidOperationException("Expected " + jsonParserToken + " , but got " + state.CurrentTokenType);
        }

        private static async Task RefillParserBuffer(Stream stream, JsonOperationContext.ManagedPinnedBuffer buffer, UnmanagedJsonParser parser, CancellationToken token = default(CancellationToken))
        {
            // Although we using here WithCancellation and passing the token,
            // the stream will stay open even after the cancellation until the entire server will be disposed.
            var read = await stream.ReadAsync(buffer.Buffer.Array, buffer.Buffer.Offset, buffer.Buffer.Count, token).WithCancellation(token);
            if (read == 0)
                ThrowUnexpectedEndOfStream();
            parser.SetBuffer(buffer, 0, read);
        }

        private static void ThrowUnexpectedEndOfStream()
        {
            throw new EndOfStreamException();
        }

    }
}