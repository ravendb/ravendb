using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Extensions;
using Raven.Server.Documents.Patch;
using Raven.Server.ServerWide;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Threading;
using Sparrow.Utils;

namespace Raven.Server.Documents.Handlers
{
    public class BatchRequestParser
    {
        public struct CommandData
        {
            public CommandType Type;
            public string Id;
            public BlittableJsonReaderObject Document;
            public PatchRequest Patch;
            public BlittableJsonReaderObject PatchArgs;
            public PatchRequest PatchIfMissing;
            public BlittableJsonReaderObject PatchIfMissingArgs;
            public LazyStringValue ChangeVector;
            public bool IdPrefixed;
            public long Index;
            public bool FromEtl;
            public bool ReturnDocument;

            [JsonIgnore]
            public PatchDocumentCommand PatchCommand;

            #region Attachment

            public string Name;
            public string DestinationId;
            public string DestinationName;
            public string ContentType;
            public AttachmentType AttachmentType;

            #endregion

            #region Counter

            public DocumentCountersOperation Counters;

            #endregion
        }


        private static readonly CommandData[] Empty = new CommandData[0];
        private static readonly int MaxSizeOfCommandsInBatchToCache = 128;

        [ThreadStatic]
        private static Stack<CommandData[]> _cache;

        static BatchRequestParser()
        {
            ThreadLocalCleanup.ReleaseThreadLocalState += () =>
            {
                _cache?.Clear();
                _cache = null;
            };
        }
        public static void ReturnBuffer(ArraySegment<CommandData> cmds)
        {
            Array.Clear(cmds.Array, cmds.Offset, cmds.Count);
            ReturnBuffer(cmds.Array);
        }

        private static void ReturnBuffer(CommandData[] cmds)
        {
            if (cmds.Length > MaxSizeOfCommandsInBatchToCache)
                return;
            if (_cache == null)
                _cache = new Stack<CommandData[]>();

            if (_cache.Count > 1024)
                return;
            _cache.Push(cmds);
        }


        public static async Task BuildCommandsAsync(JsonOperationContext ctx, BatchHandler.MergedBatchCommand command, Stream stream,
            DocumentDatabase database, ServerStore serverStore)
        {
            CommandData[] cmds = Empty;
            List<string> identities = null;
            List<int> positionInListToCommandIndex = null;

            int index = -1;
            var state = new JsonParserState();
            using (ctx.GetManagedBuffer(out JsonOperationContext.ManagedPinnedBuffer buffer))
            using (var parser = new UnmanagedJsonParser(ctx, state, "bulk_docs"))
            {
                while (parser.Read() == false)
                    await RefillParserBuffer(stream, buffer, parser);

                if (state.CurrentTokenType != JsonParserToken.StartObject)
                    ThrowUnexpectedToken(JsonParserToken.StartObject, state);

                while (parser.Read() == false)
                    await RefillParserBuffer(stream, buffer, parser);

                if (state.CurrentTokenType != JsonParserToken.String)
                    ThrowUnexpectedToken(JsonParserToken.String, state);

                if (GetLongFromStringBuffer(state) != 8314892176759549763) // Commands
                    ThrowUnexpectedToken(JsonParserToken.String, state);

                while (parser.Read() == false)
                    await RefillParserBuffer(stream, buffer, parser);

                if (state.CurrentTokenType != JsonParserToken.StartArray)
                    ThrowUnexpectedToken(JsonParserToken.StartArray, state);

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

                    var commandData = await ReadSingleCommand(ctx, stream, state, parser, buffer, default);

                    if (commandData.Type == CommandType.PATCH)
                    {
                        commandData.PatchCommand =
                            new PatchDocumentCommand(ctx, commandData.Id, commandData.ChangeVector,
                                false,
                                (commandData.Patch, commandData.PatchArgs),
                                (commandData.PatchIfMissing, commandData.PatchIfMissingArgs),
                                database,
                                false,
                                false,
                                true
                            );
                    }

                    if (commandData.Type == CommandType.PUT && string.IsNullOrEmpty(commandData.Id) == false && commandData.Id[commandData.Id.Length - 1] == '|')
                    {
                        if (identities == null)
                        {
                            identities = new List<string>();
                            positionInListToCommandIndex = new List<int>();
                        }
                        // queue identities requests in order to send them at once to the leader (using List for simplicity)
                        identities.Add(commandData.Id);
                        positionInListToCommandIndex.Add(index);
                    }

                    cmds[index] = commandData;
                }

                if (identities != null)
                {
                    await GetIdentitiesValues(ctx,
                        database,
                        serverStore,
                        identities,
                        positionInListToCommandIndex,
                        cmds);
                }

                command.ParsedCommands = new ArraySegment<CommandData>(cmds, 0, index + 1);
                if (await IsClusterTransaction(stream, parser, buffer, state))
                    command.IsClusterTransaction = true;
            }
        }

        private static async Task<bool> IsClusterTransaction(Stream stream, UnmanagedJsonParser parser, JsonOperationContext.ManagedPinnedBuffer buffer, JsonParserState state)
        {
            while (parser.Read() == false)
                await RefillParserBuffer(stream, buffer, parser);

            if (ReadClusterTransactionProperty(state))
            {
                while (parser.Read() == false)
                    await RefillParserBuffer(stream, buffer, parser);

                return GetStringPropertyValue(state) == nameof(TransactionMode.ClusterWide);
            }

            return false;
        }

        private static unsafe bool ReadClusterTransactionProperty(JsonParserState state)
        {
            return state.CurrentTokenType == JsonParserToken.String &&
                   state.StringSize == nameof(TransactionMode).Length &&
                   GetLongFromStringBuffer(state) == 8386654079495008852 && // Transact
                   *(int*)(state.StringBuffer + sizeof(long)) == 1299083113 && // ionM
                   *(short*)(state.StringBuffer + sizeof(long) + sizeof(int)) == 25711 && // od
                   *(state.StringBuffer + sizeof(long) + sizeof(int) + sizeof(short)) == (byte)'e';
        }

        private static async Task GetIdentitiesValues(JsonOperationContext ctx, DocumentDatabase database, ServerStore serverStore,
            List<string> identities, List<int> positionInListToCommandIndex, CommandData[] cmds)
        {
            var newIds = await serverStore.GenerateClusterIdentitiesBatchAsync(database.Name, identities);
            Debug.Assert(newIds.Count == identities.Count);

            var emptyChangeVector = ctx.GetLazyString("");

            for (var index = 0; index < positionInListToCommandIndex.Count; index++)
            {
                var value = positionInListToCommandIndex[index];
                cmds[value].Id = cmds[value].Id.Substring(0, cmds[value].Id.Length - 1) + "/" + newIds[index];

                if (string.IsNullOrEmpty(cmds[value].ChangeVector) == false)
                    ThrowInvalidUsageOfChangeVectorWithIdentities(cmds[value]);
                cmds[value].ChangeVector = emptyChangeVector;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static unsafe long GetLongFromStringBuffer(JsonParserState state)
        {
            return *(long*)state.StringBuffer;
        }

        public class ReadMany : IDisposable
        {
            private readonly Stream _stream;
            private readonly UnmanagedJsonParser _parser;
            private readonly JsonOperationContext.ManagedPinnedBuffer _buffer;
            private readonly JsonParserState _state;
            private readonly CancellationToken _token;

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
                    return new CommandData { Type = CommandType.None };

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
                    case CommandPropertyName.Type:
                        while (parser.Read() == false)
                            await RefillParserBuffer(stream, buffer, parser, token);
                        if (state.CurrentTokenType != JsonParserToken.String)
                        {
                            ThrowUnexpectedToken(JsonParserToken.String, state);
                        }
                        commandData.Type = GetCommandType(state, ctx);
                        break;
                    case CommandPropertyName.Id:
                        while (parser.Read() == false)
                            await RefillParserBuffer(stream, buffer, parser, token);
                        switch (state.CurrentTokenType)
                        {
                            case JsonParserToken.Null:
                                commandData.Id = null;
                                break;
                            case JsonParserToken.String:
                                commandData.Id = GetStringPropertyValue(state);
                                break;
                            default:
                                ThrowUnexpectedToken(JsonParserToken.String, state);
                                break;
                        }
                        break;
                    case CommandPropertyName.Name:
                        while (parser.Read() == false)
                            await RefillParserBuffer(stream, buffer, parser, token);
                        switch (state.CurrentTokenType)
                        {
                            case JsonParserToken.Null:
                                commandData.Name = null;
                                break;
                            case JsonParserToken.String:
                                commandData.Name = GetStringPropertyValue(state);
                                break;
                            default:
                                ThrowUnexpectedToken(JsonParserToken.String, state);
                                break;
                        }
                        break;
                    case CommandPropertyName.DestinationId:
                        while (parser.Read() == false)
                            await RefillParserBuffer(stream, buffer, parser, token);
                        switch (state.CurrentTokenType)
                        {
                            case JsonParserToken.Null:
                                commandData.DestinationId = null;
                                break;
                            case JsonParserToken.String:
                                commandData.DestinationId = GetStringPropertyValue(state);
                                break;
                            default:
                                ThrowUnexpectedToken(JsonParserToken.String, state);
                                break;
                        }
                        break;
                    case CommandPropertyName.DestinationName:
                        while (parser.Read() == false)
                            await RefillParserBuffer(stream, buffer, parser, token);
                        switch (state.CurrentTokenType)
                        {
                            case JsonParserToken.Null:
                                commandData.DestinationName = null;
                                break;
                            case JsonParserToken.String:
                                commandData.DestinationName = GetStringPropertyValue(state);
                                break;
                            default:
                                ThrowUnexpectedToken(JsonParserToken.String, state);
                                break;
                        }
                        break;
                    case CommandPropertyName.ContentType:
                        while (parser.Read() == false)
                            await RefillParserBuffer(stream, buffer, parser, token);
                        switch (state.CurrentTokenType)
                        {
                            case JsonParserToken.Null:
                                commandData.ContentType = string.Empty;
                                break;
                            case JsonParserToken.String:
                                commandData.ContentType = GetStringPropertyValue(state);
                                break;
                            default:
                                ThrowUnexpectedToken(JsonParserToken.String, state);
                                break;
                        }
                        break;
                    case CommandPropertyName.Document:
                        while (parser.Read() == false)
                            await RefillParserBuffer(stream, buffer, parser, token);
                        commandData.Document = await ReadJsonObject(ctx, stream, commandData.Id, parser, state, buffer, token);
                        break;
                    case CommandPropertyName.Patch:
                        while (parser.Read() == false)
                            await RefillParserBuffer(stream, buffer, parser, token);
                        var patch = await ReadJsonObject(ctx, stream, commandData.Id, parser, state, buffer, token);
                        commandData.Patch = PatchRequest.Parse(patch, out commandData.PatchArgs);
                        break;
                    case CommandPropertyName.PatchIfMissing:
                        while (parser.Read() == false)
                            await RefillParserBuffer(stream, buffer, parser, token);
                        var patchIfMissing = await ReadJsonObject(ctx, stream, commandData.Id, parser, state, buffer, token);
                        commandData.PatchIfMissing = PatchRequest.Parse(patchIfMissing, out commandData.PatchIfMissingArgs);
                        break;
                    case CommandPropertyName.ChangeVector:
                        while (parser.Read() == false)
                            await RefillParserBuffer(stream, buffer, parser, token);
                        if (state.CurrentTokenType == JsonParserToken.Null)
                        {
                            commandData.ChangeVector = null;
                        }
                        else
                        {
                            if (state.CurrentTokenType != JsonParserToken.String)
                            {
                                ThrowUnexpectedToken(JsonParserToken.String, state);
                            }

                            commandData.ChangeVector = GetLazyStringValue(ctx, state);
                        }
                        break;
                    case CommandPropertyName.Index:
                        while (parser.Read() == false)
                            await RefillParserBuffer(stream, buffer, parser, token);
                        if (state.CurrentTokenType != JsonParserToken.Integer)
                        {
                            ThrowUnexpectedToken(JsonParserToken.True, state);
                        }
                        commandData.Index = state.Long;

                        break;
                    case CommandPropertyName.IdPrefixed:
                        while (parser.Read() == false)
                            await RefillParserBuffer(stream, buffer, parser, token);

                        if (state.CurrentTokenType != JsonParserToken.True && state.CurrentTokenType != JsonParserToken.False)
                        {
                            ThrowUnexpectedToken(JsonParserToken.True, state);
                        }

                        commandData.IdPrefixed = state.CurrentTokenType == JsonParserToken.True;
                        break;
                    case CommandPropertyName.ReturnDocument:
                        while (parser.Read() == false)
                            await RefillParserBuffer(stream, buffer, parser, token);

                        if (state.CurrentTokenType != JsonParserToken.True && state.CurrentTokenType != JsonParserToken.False)
                        {
                            ThrowUnexpectedToken(JsonParserToken.True, state);
                        }

                        commandData.ReturnDocument = state.CurrentTokenType == JsonParserToken.True;
                        break;
                    case CommandPropertyName.Counters:
                        while (parser.Read() == false)
                            await RefillParserBuffer(stream, buffer, parser, token);
                        var counterOps = await ReadJsonObject(ctx, stream, commandData.Id, parser, state, buffer, token);
                        commandData.Counters = DocumentCountersOperation.Parse(counterOps);
                        break;
                    case CommandPropertyName.FromEtl:
                        while (parser.Read() == false)
                            await RefillParserBuffer(stream, buffer, parser, token);

                        if (state.CurrentTokenType != JsonParserToken.True && state.CurrentTokenType != JsonParserToken.False)
                        {
                            ThrowUnexpectedToken(JsonParserToken.True, state);
                        }

                        commandData.FromEtl = state.CurrentTokenType == JsonParserToken.True;
                        break;
                    case CommandPropertyName.AttachmentType:
                        while (parser.Read() == false)
                            await RefillParserBuffer(stream, buffer, parser, token);
                        if (state.CurrentTokenType == JsonParserToken.Null)
                        {
                            commandData.AttachmentType = AttachmentType.Document;
                        }
                        else
                        {
                            if (state.CurrentTokenType != JsonParserToken.String)
                            {
                                ThrowUnexpectedToken(JsonParserToken.String, state);
                            }

                            commandData.AttachmentType = GetAttachmentType(state, ctx);
                        }
                        break;
                    case CommandPropertyName.NoSuchProperty:
                        // unknown command - ignore it
                        while (parser.Read() == false)
                            await RefillParserBuffer(stream, buffer, parser, token);
                        if (state.CurrentTokenType == JsonParserToken.StartObject ||
                            state.CurrentTokenType == JsonParserToken.StartArray)
                        {
                            await ReadJsonObject(ctx, stream, commandData.Id, parser, state, buffer, token);
                        }
                        break;
                }
            }

            switch (commandData.Type)
            {
                case CommandType.None:
                    ThrowInvalidType();
                    break;
                case CommandType.PUT:
                    if (commandData.Document == null)
                        ThrowMissingDocumentProperty();
                    break;
                case CommandType.PATCH:
                    if (commandData.Patch == null)
                        ThrowMissingPatchProperty();
                    break;
                case CommandType.AttachmentPUT:
                    if (commandData.Name == null)
                        ThrowMissingNameProperty();
                    break;
                case CommandType.Counters:
                    if (commandData.Counters == null)
                        ThrowMissingNameProperty();
                    break;
            }

            return commandData;
        }

        private static CommandData[] IncreaseSizeOfCommandsBuffer(int index, CommandData[] cmds)
        {
            if (cmds.Length > MaxSizeOfCommandsInBatchToCache)
            {
                Array.Resize(ref cmds, Math.Max(index + 8, cmds.Length * 2));
                return cmds;
            }

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

        private static void ThrowInvalidType()
        {
            throw new InvalidOperationException($"Command must have a valid '{nameof(CommandData.Type)}' property");
        }

        private static void ThrowMissingDocumentProperty()
        {
            throw new InvalidOperationException($"PUT command must have a '{nameof(CommandData.Document)}' property");
        }

        private static void ThrowMissingPatchProperty()
        {
            throw new InvalidOperationException($"PUT command must have a '{nameof(CommandData.Patch)}' property");
        }

        private static void ThrowMissingNameProperty()
        {
            throw new InvalidOperationException($"Attachment PUT command must have a '{nameof(CommandData.Name)}' property");
        }

        private static async Task<BlittableJsonReaderObject> ReadJsonObject(JsonOperationContext ctx, Stream stream, string id, UnmanagedJsonParser parser,
            JsonParserState state, JsonOperationContext.ManagedPinnedBuffer buffer, CancellationToken token)
        {
            if (state.CurrentTokenType == JsonParserToken.Null)
                return null;

            BlittableJsonReaderObject reader;
            using (var builder = new BlittableJsonDocumentBuilder(ctx,
                BlittableJsonDocumentBuilder.UsageMode.ToDisk,
                id, parser, state, modifier: new BlittableMetadataModifier(ctx)))
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

        private static unsafe string GetStringPropertyValue(JsonParserState state)
        {
            return Encoding.UTF8.GetString(state.StringBuffer, state.StringSize);
        }

        private static unsafe LazyStringValue GetLazyStringValue(JsonOperationContext ctx, JsonParserState state)
        {
            return ctx.GetLazyString(Encodings.Utf8.GetString(state.StringBuffer, state.StringSize));
        }

        private enum CommandPropertyName
        {
            NoSuchProperty,
            Type,
            Id,
            Document,
            ChangeVector,
            Patch,
            PatchIfMissing,
            IdPrefixed,
            Index,
            ReturnDocument,

            #region Attachment

            Name,
            DestinationId,
            DestinationName,
            ContentType,
            AttachmentType,

            #endregion

            #region Counter

            Counters,

            #endregion

            FromEtl

            // other properties are ignore (for legacy support)
        }

        private static unsafe CommandPropertyName GetPropertyType(JsonParserState state)
        {
            // here we confirm that the value is matching our expectation, in order to save CPU instructions
            // we compare directly against the precomputed values
            switch (state.StringSize)
            {
                case 2:
                    if (*(short*)state.StringBuffer != 25673)
                        return CommandPropertyName.NoSuchProperty;
                    return CommandPropertyName.Id;

                case 8:
                    if (*(long*)state.StringBuffer == 8318823012450529091)
                        return CommandPropertyName.Counters;
                    if (*(long*)state.StringBuffer != 8389754676633104196)
                        return CommandPropertyName.NoSuchProperty;
                    return CommandPropertyName.Document;

                case 4:
                    if (*(int*)state.StringBuffer == 1701869908)
                        return CommandPropertyName.Type;
                    if (*(int*)state.StringBuffer == 1701667150)
                        return CommandPropertyName.Name;
                    return CommandPropertyName.NoSuchProperty;

                case 5:
                    if (*(int*)state.StringBuffer == 1668571472 &&
                        state.StringBuffer[4] == (byte)'h')
                        return CommandPropertyName.Patch;
                    if (*(int*)state.StringBuffer == 1701080649 &&
                        state.StringBuffer[4] == (byte)'x')
                        return CommandPropertyName.Index;
                    return CommandPropertyName.NoSuchProperty;
                case 14:
                    if (*(int*)state.StringBuffer == 1668571472 &&
                        *(long*)(state.StringBuffer + sizeof(int)) == 7598543892411468136 &&
                        *(short*)(state.StringBuffer + sizeof(int) + sizeof(long)) == 26478)
                        return CommandPropertyName.PatchIfMissing;
                    
                    if (*(int*)state.StringBuffer == 1970562386 &&
                        *(long*)(state.StringBuffer + sizeof(int)) == 7308626840221150834 &&
                        *(short*)(state.StringBuffer + sizeof(int) + sizeof(long)) == 29806)
                        return CommandPropertyName.ReturnDocument;

                    if (*(int*)state.StringBuffer == 1635021889 && 
                        *(long*)(state.StringBuffer + sizeof(int)) == 8742740794129868899 &&
                        *(short*)(state.StringBuffer + sizeof(int) + sizeof(long)) == 25968)
                        return CommandPropertyName.AttachmentType;

                    return CommandPropertyName.NoSuchProperty;
                case 10:
                    if (*(long*)state.StringBuffer == 8676578743001572425 &&
                        *(short*)(state.StringBuffer + sizeof(long)) == 25701)
                        return CommandPropertyName.IdPrefixed;
                    return CommandPropertyName.NoSuchProperty;

                case 11:
                    if (*(long*)state.StringBuffer == 6085610378508529475 &&
                        *(short*)(state.StringBuffer + sizeof(long)) == 28793 &&
                        state.StringBuffer[sizeof(long) + sizeof(short)] == (byte)'e')
                        return CommandPropertyName.ContentType;
                    return CommandPropertyName.NoSuchProperty;

                case 12:
                    if (*(long*)state.StringBuffer == 7302135340735752259 &&
                        *(int*)(state.StringBuffer + sizeof(long)) == 1919906915)
                        return CommandPropertyName.ChangeVector;
                    return CommandPropertyName.NoSuchProperty;
                case 7:
                    if (*(int*)state.StringBuffer == 1836020294 &&
                        *(short*)(state.StringBuffer + sizeof(int)) == 29765 &&
                        state.StringBuffer[6] == (byte)'l')
                        return CommandPropertyName.FromEtl;

                    return CommandPropertyName.NoSuchProperty;
                case 13:
                    if (*(long*)state.StringBuffer == 8386105380344915268 &&
                        *(int*)(state.StringBuffer + sizeof(long)) == 1231974249 &&
                        state.StringBuffer[12] == (byte)'d')
                        return CommandPropertyName.DestinationId;

                    return CommandPropertyName.NoSuchProperty;
                case 15:
                    if (*(long*)state.StringBuffer == 8386105380344915268 &&
                        *(int*)(state.StringBuffer + sizeof(long)) == 1315860329 &&
                        *(short*)(state.StringBuffer + sizeof(long) + sizeof(int)) == 28001 &&
                        state.StringBuffer[14] == (byte)'e')
                        return CommandPropertyName.DestinationName;

                    return CommandPropertyName.NoSuchProperty;
                default:
                    return CommandPropertyName.NoSuchProperty;
            }
        }

        private static unsafe AttachmentType GetAttachmentType(JsonParserState state, JsonOperationContext ctx)
        {
            // here we confirm that the value is matching our expectation, in order to save CPU instructions
            // we compare directly against the precomputed values
            switch (state.StringSize)
            {
                case 8:
                    if (*(long*)state.StringBuffer == 8389754676633104196)
                        return AttachmentType.Document;
                    if (*(long*)state.StringBuffer == 7957695010998478162)
                        return AttachmentType.Revision;
                    
                    ThrowInvalidProperty(state, ctx);
                    break;
                default:
                    ThrowInvalidProperty(state, ctx);
                    break;
            }

            return 0;
        }
        
        private static unsafe CommandType GetCommandType(JsonParserState state, JsonOperationContext ctx)
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

                case 13:
                    if (*(long*)state.StringBuffer != 7308612546338255937 ||
                        *(int*)(state.StringBuffer + sizeof(long)) != 1431336046 ||
                        state.StringBuffer[sizeof(long) + sizeof(int)] != (byte)'T')
                        ThrowInvalidProperty(state, ctx);

                    return CommandType.AttachmentPUT;
                case 14:
                    if (*(long*)state.StringBuffer == 7308612546338255937 &&
                        *(int*)(state.StringBuffer + sizeof(long)) == 1329820782 &&
                        *(short*)(state.StringBuffer + sizeof(long) + sizeof(int)) == 22864)
                        return CommandType.AttachmentCOPY;
                        
                    if (*(long*)state.StringBuffer == 7308612546338255937 &&
                        *(int*)(state.StringBuffer + sizeof(long)) == 1330476142 &&
                        *(short*)(state.StringBuffer + sizeof(long) + sizeof(int)) == 17750)
                        return CommandType.AttachmentMOVE;

                    ThrowInvalidProperty(state, ctx);
                    return CommandType.None;
                case 16:
                    if (*(long*)state.StringBuffer == 7308612546338255937 &&
                        *(long*)(state.StringBuffer + sizeof(long)) == 4995694080542667886)
                        return CommandType.AttachmentDELETE;

                    ThrowInvalidProperty(state, ctx);
                    return CommandType.None;
                case 8:
                    if (*(long*)state.StringBuffer != 8318823012450529091)
                        ThrowInvalidProperty(state, ctx);

                    return CommandType.Counters;

                case 18:
                    if (*(long*)state.StringBuffer != 5000528724088418115 ||
                        *(long*)(state.StringBuffer + sizeof(long)) != 5793150219460305784 ||
                        *(short*)(state.StringBuffer + sizeof(long) + sizeof(long)) != 21589)
                    {
                        ThrowInvalidProperty(state, ctx);
                    }
                    return CommandType.CompareExchangePUT;

                case 21:
                    if (*(long*)state.StringBuffer != 5000528724088418115 ||
                        *(long*)(state.StringBuffer + sizeof(long)) != 4928459091005170552 ||
                        *(int*)(state.StringBuffer + sizeof(long) + sizeof(long)) != 1413827653 ||
                        state.StringBuffer[sizeof(long) + sizeof(long) + sizeof(int)] != (byte)'E')
                    {
                        ThrowInvalidProperty(state, ctx);
                    }
                    return CommandType.CompareExchangeDELETE;

                default:
                    ThrowInvalidProperty(state, ctx);
                    return CommandType.None;
            }
        }

        private static void ThrowInvalidUsageOfChangeVectorWithIdentities(CommandData commandData)
        {
            throw new InvalidOperationException($"You cannot use change vector ({commandData.ChangeVector}) " +
                                                $"when using identity in the document ID ({commandData.Id}).");
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

        private static async Task RefillParserBuffer(Stream stream, JsonOperationContext.ManagedPinnedBuffer buffer, UnmanagedJsonParser parser, CancellationToken token = default)
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
