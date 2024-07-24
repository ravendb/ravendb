using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Documents.Session;
using Raven.Server.Documents.Handlers.Batches.Commands;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.Exceptions;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server.Strings;
using Sparrow.Utils;
using PatchRequest = Raven.Server.Documents.Patch.PatchRequest;

namespace Raven.Server.Documents.Handlers.Batches
{
    public sealed class BatchRequestParser
    {
        internal static BatchRequestParser Instance = new BatchRequestParser();

        public sealed class CommandData : IBatchCommandData
        {
            public CommandType Type { get; set; }
            public string Id { get; set; }
            public BlittableJsonReaderArray Ids;
            public BlittableJsonReaderObject Document;
            public PatchRequest Patch;
            public List<JsonPatchCommand.Command> JsonPatchCommands;
            public BlittableJsonReaderObject PatchArgs;
            public PatchRequest PatchIfMissing;
            public BlittableJsonReaderObject CreateIfMissing;
            public BlittableJsonReaderObject PatchIfMissingArgs;
            public LazyStringValue ChangeVector;
            public LazyStringValue OriginalChangeVector;
            public bool IdPrefixed;
            public long Index;
            public bool FromEtl;
            public bool ReturnDocument;

            public bool SeenCounters;
            public bool SeenAttachments;
            public bool SeenTimeSeries;

            public ForceRevisionStrategy ForceRevisionCreationStrategy;

            [JsonIgnore]
            public PatchDocumentCommandBase PatchCommand;

            [JsonIgnore]
            public JsonPatchCommand JsonPatchCommand;

            #region Attachment

            public string Name;
            public string DestinationId;
            public string DestinationName;
            public string ContentType;
            public AttachmentType AttachmentType;
            public AttachmentFlags Flags;
            public long Size;
            public DateTime? RetiredAt;
            public string Hash;
            public MergedBatchCommand.AttachmentStream AttachmentStream { get; set; }// used for bulk insert only

            #endregion Attachment

            #region Counter

            public DocumentCountersOperation Counters;

            #endregion Counter

            #region Time Series

            public TimeSeriesOperation TimeSeries;
            public DateTime? From;
            public DateTime? To;

            #endregion Time Series

            #region ravendata

            public long ContentLength { get; set; }

            #endregion ravendata

        }


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

        public async Task<bool> IsClusterTransaction(Stream stream, UnmanagedJsonParser parser, JsonOperationContext.MemoryBuffer buffer, JsonParserState state)
        {
            while (parser.Read() == false)
                await RefillParserBuffer(stream, buffer, parser).ConfigureAwait(false);

            if (ReadClusterTransactionProperty(state))
            {
                while (parser.Read() == false)
                    await RefillParserBuffer(stream, buffer, parser).ConfigureAwait(false);

                return GetStringPropertyValue(state) == nameof(TransactionMode.ClusterWide);
            }

            return false;
        }

        private static unsafe bool ReadClusterTransactionProperty(JsonParserState state)
        {
            return state.CurrentTokenType == JsonParserToken.String &&
                   "TransactionMode"u8.IsEqualConstant(state.StringBuffer, state.StringSize);
        }

        public async Task<CommandData> ReadSingleCommand(
            JsonOperationContext ctx,
            Stream stream,
            JsonParserState state,
            UnmanagedJsonParser parser,
            JsonOperationContext.MemoryBuffer buffer,
            BlittableMetadataModifier modifier,
            CancellationToken token)
        {
            var commandData = new CommandData();
            if (state.CurrentTokenType != JsonParserToken.StartObject)
            {
                ThrowUnexpectedToken(JsonParserToken.StartObject, state);
            }

            CommandParsingObserver?.OnCommandStart(parser);

            while (true)
            {
                while (parser.Read() == false)
                    await RefillParserBuffer(stream, buffer, parser, token).ConfigureAwait(false);

                if (state.CurrentTokenType == JsonParserToken.EndObject)
                {
                    CommandParsingObserver?.OnCommandEnd(parser);
                    break;
                }

                if (state.CurrentTokenType != JsonParserToken.String)
                {
                    ThrowUnexpectedToken(JsonParserToken.String, state);
                }
                switch (GetPropertyType(state))
                {
                    case CommandPropertyName.Type:
                        while (parser.Read() == false)
                            await RefillParserBuffer(stream, buffer, parser, token).ConfigureAwait(false);
                        if (state.CurrentTokenType != JsonParserToken.String)
                        {
                            ThrowUnexpectedToken(JsonParserToken.String, state);
                        }
                        commandData.Type = GetCommandType(state, ctx);
                        break;

                    case CommandPropertyName.Id:
                        while (parser.Read() == false)
                            await RefillParserBuffer(stream, buffer, parser, token).ConfigureAwait(false);

                        switch (state.CurrentTokenType)
                        {
                            case JsonParserToken.Null:
                                commandData.Id = null;
                                CommandParsingObserver?.OnId(parser, 4, isNull: true);
                                break;

                            case JsonParserToken.String:
                                CommandParsingObserver?.OnId(parser, state.StringSize, isNull: false);
                                commandData.Id = GetStringPropertyValue(state);
                                break;

                            default:
                                ThrowUnexpectedToken(JsonParserToken.String, state);
                                break;
                        }


                        break;

                    case CommandPropertyName.Ids:

                        CommandParsingObserver?.OnIdsStart(parser);

                        commandData.Ids = await ReadJsonArray(ctx, stream, parser, state, buffer, token).ConfigureAwait(false);

                        CommandParsingObserver?.OnIdsEnd(parser);
                        break;

                    case CommandPropertyName.Name:
                        while (parser.Read() == false)
                            await RefillParserBuffer(stream, buffer, parser, token).ConfigureAwait(false);
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
                            await RefillParserBuffer(stream, buffer, parser, token).ConfigureAwait(false);
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
                    case CommandPropertyName.From:
                        while (parser.Read() == false)
                            await RefillParserBuffer(stream, buffer, parser, token).ConfigureAwait(false);
                        switch (state.CurrentTokenType)
                        {
                            case JsonParserToken.Null:
                                commandData.From = null;
                                break;
                            case JsonParserToken.String:
                                commandData.From = DateTime.Parse(GetStringPropertyValue(state)).ToUniversalTime();
                                break;
                            default:
                                ThrowUnexpectedToken(JsonParserToken.String, state);
                                break;
                        }
                        break;
                    case CommandPropertyName.To:
                        while (parser.Read() == false)
                            await RefillParserBuffer(stream, buffer, parser, token).ConfigureAwait(false);
                        switch (state.CurrentTokenType)
                        {
                            case JsonParserToken.Null:
                                commandData.To = null;
                                break;
                            case JsonParserToken.String:
                                commandData.To = DateTime.Parse(GetStringPropertyValue(state)).ToUniversalTime();
                                break;
                            default:
                                ThrowUnexpectedToken(JsonParserToken.String, state);
                                break;
                        }
                        break;
                    case CommandPropertyName.DestinationName:
                        while (parser.Read() == false)
                            await RefillParserBuffer(stream, buffer, parser, token).ConfigureAwait(false);
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
                            await RefillParserBuffer(stream, buffer, parser, token).ConfigureAwait(false);
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
                    case CommandPropertyName.Flags:
                        while (parser.Read() == false)
                            await RefillParserBuffer(stream, buffer, parser, token).ConfigureAwait(false);
                        if (state.CurrentTokenType != JsonParserToken.String)
                        {
                            ThrowUnexpectedToken(JsonParserToken.String, state);
                        }
                        commandData.Flags = GetAttachmentFlag(state, ctx);
                        break;
                    case CommandPropertyName.Size:
                        while (parser.Read() == false)
                            await RefillParserBuffer(stream, buffer, parser, token).ConfigureAwait(false);
                        if (state.CurrentTokenType != JsonParserToken.Integer)
                        {
                            ThrowUnexpectedToken(JsonParserToken.Integer, state);
                        }
                        commandData.Size = state.Long;

                        break;
                    case CommandPropertyName.RetiredAt:
                        while (parser.Read() == false)
                            await RefillParserBuffer(stream, buffer, parser, token).ConfigureAwait(false);
                        switch (state.CurrentTokenType)
                        {
                            case JsonParserToken.Null:
                                commandData.RetiredAt = null;
                                break;

                            case JsonParserToken.String:
                                commandData.RetiredAt = DateTime.Parse(GetStringPropertyValue(state)).ToUniversalTime();
                                break;

                            default:
                                ThrowUnexpectedToken(JsonParserToken.String, state);
                                break;
                        }

                        break;
                    case CommandPropertyName.Hash:
                        while (parser.Read() == false)
                            await RefillParserBuffer(stream, buffer, parser, token).ConfigureAwait(false);
                        switch (state.CurrentTokenType)
                        {
                            case JsonParserToken.Null:
                                commandData.Hash = null;
                                break;

                            case JsonParserToken.String:
                                commandData.Hash = GetStringPropertyValue(state);
                                break;

                            default:
                                ThrowUnexpectedToken(JsonParserToken.String, state);
                                break;
                        }
                        break;

                    case CommandPropertyName.Document:
                        while (parser.Read() == false)
                            await RefillParserBuffer(stream, buffer, parser, token).ConfigureAwait(false);
                        commandData.Document = await ReadJsonObject(ctx, stream, commandData.Id, parser, state, buffer, modifier, token).ConfigureAwait(false);
                        commandData.SeenAttachments = modifier.SeenAttachments;
                        commandData.SeenCounters = modifier.SeenCounters;
                        commandData.SeenTimeSeries = modifier.SeenTimeSeries;

                        break;

                    case CommandPropertyName.Patch:
                        while (parser.Read() == false)
                            await RefillParserBuffer(stream, buffer, parser, token).ConfigureAwait(false);
                        var patch = await ReadJsonObject(ctx, stream, commandData.Id, parser, state, buffer, modifier, token).ConfigureAwait(false);
                        commandData.Patch = PatchRequest.Parse(patch, out commandData.PatchArgs);
                        break;

                    case CommandPropertyName.JsonPatch:
                        while (parser.Read() == false)
                            await RefillParserBuffer(stream, buffer, parser, token).ConfigureAwait(false);
                        var jsonPatch = await ReadJsonObject(ctx, stream, commandData.Id, parser, state, buffer, modifier, token).ConfigureAwait(false);
                        commandData.JsonPatchCommands = JsonPatchCommand.Parse(jsonPatch);
                        break;

                    case CommandPropertyName.TimeSeries:
                        while (parser.Read() == false)
                            await RefillParserBuffer(stream, buffer, parser, token).ConfigureAwait(false);

                        using (var timeSeriesOperations = await ReadJsonObject(ctx, stream, commandData.Id, parser, state, buffer, modifier, token).ConfigureAwait(false))
                        {
                            commandData.TimeSeries = commandData.Type == CommandType.TimeSeriesBulkInsert ?
                                TimeSeriesOperation.ParseForBulkInsert(timeSeriesOperations) :
                                TimeSeriesOperation.Parse(timeSeriesOperations);
                        }

                        break;

                    case CommandPropertyName.CreateIfMissing:
                        while (parser.Read() == false)
                            await RefillParserBuffer(stream, buffer, parser, token).ConfigureAwait(false);
                        var createIfMissing = await ReadJsonObject(ctx, stream, commandData.Id, parser, state, buffer, modifier, token).ConfigureAwait(false);
                        commandData.CreateIfMissing = createIfMissing;
                        break;
                    case CommandPropertyName.PatchIfMissing:
                        while (parser.Read() == false)
                            await RefillParserBuffer(stream, buffer, parser, token).ConfigureAwait(false);
                        var patchIfMissing = await ReadJsonObject(ctx, stream, commandData.Id, parser, state, buffer, modifier, token).ConfigureAwait(false);
                        commandData.PatchIfMissing = PatchRequest.Parse(patchIfMissing, out commandData.PatchIfMissingArgs);
                        break;

                    case CommandPropertyName.ChangeVector:
                        while (parser.Read() == false)
                            await RefillParserBuffer(stream, buffer, parser, token).ConfigureAwait(false);
                        if (state.CurrentTokenType == JsonParserToken.Null)
                        {
                            commandData.ChangeVector = null;

                            CommandParsingObserver?.OnNullChangeVector(parser);
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
                    case CommandPropertyName.OriginalChangeVector:
                        while (parser.Read() == false)
                            await RefillParserBuffer(stream, buffer, parser, token).ConfigureAwait(false);
                        if (state.CurrentTokenType == JsonParserToken.Null)
                        {
                            commandData.OriginalChangeVector = null;
                        }
                        else
                        {
                            if (state.CurrentTokenType != JsonParserToken.String)
                            {
                                ThrowUnexpectedToken(JsonParserToken.String, state);
                            }

                            commandData.OriginalChangeVector = GetLazyStringValue(ctx, state);
                        }
                        break;


                    case CommandPropertyName.Index:
                        while (parser.Read() == false)
                            await RefillParserBuffer(stream, buffer, parser, token).ConfigureAwait(false);
                        if (state.CurrentTokenType != JsonParserToken.Integer)
                        {
                            ThrowUnexpectedToken(JsonParserToken.Integer, state);
                        }
                        commandData.Index = state.Long;

                        break;

                    case CommandPropertyName.IdPrefixed:
                        while (parser.Read() == false)
                            await RefillParserBuffer(stream, buffer, parser, token).ConfigureAwait(false);

                        if (state.CurrentTokenType != JsonParserToken.True && state.CurrentTokenType != JsonParserToken.False)
                        {
                            ThrowUnexpectedToken(JsonParserToken.True, state);
                        }

                        commandData.IdPrefixed = state.CurrentTokenType == JsonParserToken.True;
                        break;

                    case CommandPropertyName.ReturnDocument:
                        while (parser.Read() == false)
                            await RefillParserBuffer(stream, buffer, parser, token).ConfigureAwait(false);

                        if (state.CurrentTokenType != JsonParserToken.True && state.CurrentTokenType != JsonParserToken.False)
                        {
                            ThrowUnexpectedToken(JsonParserToken.True, state);
                        }

                        commandData.ReturnDocument = state.CurrentTokenType == JsonParserToken.True;
                        break;

                    case CommandPropertyName.Counters:
                        while (parser.Read() == false)
                            await RefillParserBuffer(stream, buffer, parser, token).ConfigureAwait(false);

                        var counterOps = await ReadJsonObject(ctx, stream, commandData.Id, parser, state, buffer, modifier, token).ConfigureAwait(false);
                        commandData.Counters = DocumentCountersOperation.Parse(counterOps);
                        break;

                    case CommandPropertyName.FromEtl:
                        while (parser.Read() == false)
                            await RefillParserBuffer(stream, buffer, parser, token).ConfigureAwait(false);

                        if (state.CurrentTokenType != JsonParserToken.True && state.CurrentTokenType != JsonParserToken.False)
                        {
                            ThrowUnexpectedToken(JsonParserToken.True, state);
                        }

                        commandData.FromEtl = state.CurrentTokenType == JsonParserToken.True;
                        break;

                    case CommandPropertyName.AttachmentType:
                        while (parser.Read() == false)
                            await RefillParserBuffer(stream, buffer, parser, token).ConfigureAwait(false);
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

                    case CommandPropertyName.ContentLength:
                        while (parser.Read() == false)
                            await RefillParserBuffer(stream, buffer, parser, token).ConfigureAwait(false);
                        if (state.CurrentTokenType != JsonParserToken.Integer)
                        {
                            ThrowUnexpectedToken(JsonParserToken.Integer, state);
                        }
                        commandData.ContentLength = state.Long;
                        break;

                    case CommandPropertyName.NoSuchProperty:
                        // unknown command - ignore it
                        while (parser.Read() == false)
                            await RefillParserBuffer(stream, buffer, parser, token).ConfigureAwait(false);
                        if (state.CurrentTokenType == JsonParserToken.StartObject ||
                            state.CurrentTokenType == JsonParserToken.StartArray)
                        {
                            await ReadJsonObject(ctx, stream, commandData.Id, parser, state, buffer, modifier, token).ConfigureAwait(false);
                        }
                        break;

                    case CommandPropertyName.ForceRevisionCreationStrategy:
                        while (parser.Read() == false)
                            await RefillParserBuffer(stream, buffer, parser, token).ConfigureAwait(false);
                        if (state.CurrentTokenType != JsonParserToken.String)
                        {
                            ThrowUnexpectedToken(JsonParserToken.String, state);
                        }

                        commandData.ForceRevisionCreationStrategy = GetEnumValue(state, ctx);
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

        public static CommandData[] IncreaseSizeOfCommandsBuffer(int index, CommandData[] cmds)
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

        [DoesNotReturn]
        private static void ThrowInvalidType()
        {
            throw new InvalidOperationException($"Command must have a valid '{nameof(CommandData.Type)}' property");
        }

        [DoesNotReturn]
        private static void ThrowMissingDocumentProperty()
        {
            throw new InvalidOperationException($"PUT command must have a '{nameof(CommandData.Document)}' property");
        }

        [DoesNotReturn]
        private static void ThrowMissingPatchProperty()
        {
            throw new InvalidOperationException($"PUT command must have a '{nameof(CommandData.Patch)}' property");
        }

        [DoesNotReturn]
        private static void ThrowMissingNameProperty()
        {
            throw new InvalidOperationException($"Attachment PUT command must have a '{nameof(CommandData.Name)}' property");
        }

        private async Task<BlittableJsonReaderArray> ReadJsonArray(
            JsonOperationContext ctx,
            Stream stream,
            UnmanagedJsonParser parser,
            JsonParserState state,
            JsonOperationContext.MemoryBuffer buffer,
            CancellationToken token)
        {
            BlittableJsonReaderArray reader;
            using (var builder = new BlittableJsonDocumentBuilder(
                ctx,
                BlittableJsonDocumentBuilder.UsageMode.ToDisk,
                "json/array", parser, state))
            {
                ctx.CachedProperties.NewDocument();
                builder.ReadArrayDocument();
                while (true)
                {
                    if (builder.Read())
                        break;

                    await RefillParserBuffer(stream, buffer, parser, token).ConfigureAwait(false);
                }

                builder.FinalizeDocument();
                reader = builder.CreateArrayReader(noCache: true);
            }

            return reader;
        }

        private async Task<BlittableJsonReaderObject> ReadJsonObject(JsonOperationContext ctx, Stream stream, string id, UnmanagedJsonParser parser,
            JsonParserState state, JsonOperationContext.MemoryBuffer buffer, IBlittableDocumentModifier modifier, CancellationToken token)
        {
            if (state.CurrentTokenType == JsonParserToken.Null)
                return null;

            BlittableJsonReaderObject reader;
            using (var builder = new BlittableJsonDocumentBuilder(ctx,
                BlittableJsonDocumentBuilder.UsageMode.ToDisk,
                id, parser, state, modifier: modifier))
            {
                ctx.CachedProperties.NewDocument();
                builder.ReadNestedObject();
                while (true)
                {
                    if (builder.Read())
                        break;

                    await RefillParserBuffer(stream, buffer, parser, token).ConfigureAwait(false);
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
            Ids,
            Document,
            ChangeVector,
            OriginalChangeVector,
            Patch,
            PatchIfMissing,
            CreateIfMissing,
            IdPrefixed,
            Index,
            ReturnDocument,
            ForceRevisionCreationStrategy,

            #region Attachment

            Name,
            DestinationId,
            DestinationName,
            ContentType,
            AttachmentType,
            Flags,
            Size,
            RetiredAt,
            Hash,
            #endregion Attachment

            #region Counter

            Counters,

            #endregion Counter

            #region TimeSeries

            TimeSeries,
            From,
            To,

            #endregion TimeSeries

            #region RavenData

            ContentLength,

            #endregion RavenData

            FromEtl,

            JsonPatch

            // other properties are ignore (for legacy support)

        }

        private static unsafe CommandPropertyName GetPropertyType(JsonParserState state)
        {
            // here we confirm that the value is matching our expectation, in order to save CPU instructions
            // we compare directly against the precomputed values
            switch (state.StringSize)
            {
                case 2:
                    if ("Id"u8.IsEqualConstant(state.StringBuffer))
                        return CommandPropertyName.Id;
                    if ("To"u8.IsEqualConstant(state.StringBuffer))
                        return CommandPropertyName.To;

                    return CommandPropertyName.NoSuchProperty;

                case 3:

                    if ("Ids"u8.IsEqualConstant(state.StringBuffer))
                        return CommandPropertyName.Ids;

                    return CommandPropertyName.NoSuchProperty;
                case 8:
                    if ("Document"u8.IsEqualConstant(state.StringBuffer))
                        return CommandPropertyName.Document;

                    if ("Counters"u8.IsEqualConstant(state.StringBuffer))
                        return CommandPropertyName.Counters;

                    return CommandPropertyName.NoSuchProperty;

                case 4:
                    if ("Type"u8.IsEqualConstant(state.StringBuffer))
                        return CommandPropertyName.Type;
                    if ("Name"u8.IsEqualConstant(state.StringBuffer))
                        return CommandPropertyName.Name;
                    if ("From"u8.IsEqualConstant(state.StringBuffer))
                        return CommandPropertyName.From;
                    if ("Size"u8.IsEqualConstant(state.StringBuffer))
                        return CommandPropertyName.Size;
                    if ("Hash"u8.IsEqualConstant(state.StringBuffer))
                        return CommandPropertyName.Hash;
                    return CommandPropertyName.NoSuchProperty;

     
                case 5:
                    if ("Index"u8.IsEqualConstant(state.StringBuffer))
                        return CommandPropertyName.Index;
                    if ("Patch"u8.IsEqualConstant(state.StringBuffer))
                        return CommandPropertyName.Patch;
                    if ("Flags"u8.IsEqualConstant(state.StringBuffer))
                        return CommandPropertyName.Flags;

                    return CommandPropertyName.NoSuchProperty;

                case 10:
                    if ("IdPrefixed"u8.IsEqualConstant(state.StringBuffer))
                        return CommandPropertyName.IdPrefixed;

                    if ("TimeSeries"u8.IsEqualConstant(state.StringBuffer))
                        return CommandPropertyName.TimeSeries;

                    return CommandPropertyName.NoSuchProperty;

                case 11:
                    if ("ContentType"u8.IsEqualConstant(state.StringBuffer))
                        return CommandPropertyName.ContentType;

                    return CommandPropertyName.NoSuchProperty;

                case 12:
                    if ("ChangeVector"u8.IsEqualConstant(state.StringBuffer))
                        return CommandPropertyName.ChangeVector;

                    return CommandPropertyName.NoSuchProperty;

                case 7:
                    if ("FromEtl"u8.IsEqualConstant(state.StringBuffer))
                        return CommandPropertyName.FromEtl;

                    return CommandPropertyName.NoSuchProperty;

                case 13:
                    if ("DestinationId"u8.IsEqualConstant(state.StringBuffer))
                        return CommandPropertyName.DestinationId;

                    if ("ContentLength"u8.IsEqualConstant(state.StringBuffer))
                        return CommandPropertyName.ContentLength;

                    return CommandPropertyName.NoSuchProperty;

                case 14:
                    if ("ReturnDocument"u8.IsEqualConstant(state.StringBuffer))
                        return CommandPropertyName.ReturnDocument;

                    if ("PatchIfMissing"u8.IsEqualConstant(state.StringBuffer))
                        return CommandPropertyName.PatchIfMissing;

                    if ("AttachmentType"u8.IsEqualConstant(state.StringBuffer))
                        return CommandPropertyName.AttachmentType;

                    return CommandPropertyName.NoSuchProperty;

                case 15:
                    if ("DestinationName"u8.IsEqualConstant(state.StringBuffer))
                        return CommandPropertyName.DestinationName;

                    if ("CreateIfMissing"u8.IsEqualConstant(state.StringBuffer))
                        return CommandPropertyName.CreateIfMissing;

                    return CommandPropertyName.NoSuchProperty;

                case 20:
                    if ("OriginalChangeVector"u8.IsEqualConstant(state.StringBuffer))
                        return CommandPropertyName.OriginalChangeVector;

                    return CommandPropertyName.NoSuchProperty;


                case 29:
                    if ("ForceRevisionCreationStrategy"u8.IsEqualConstant(state.StringBuffer))
                        return CommandPropertyName.ForceRevisionCreationStrategy;

                    return CommandPropertyName.NoSuchProperty;

                case 9:
                    if ("JsonPatch"u8.IsEqualConstant(state.StringBuffer))
                        return CommandPropertyName.JsonPatch;
                    if ("RetiredAt"u8.IsEqualConstant(state.StringBuffer))
                        return CommandPropertyName.RetiredAt;

                    return CommandPropertyName.NoSuchProperty;
                default:
                    return CommandPropertyName.NoSuchProperty;
            }
        }

        private static unsafe ForceRevisionStrategy GetEnumValue(JsonParserState state, JsonOperationContext ctx)
        {
            switch (state.StringSize)
            {
                case 6:
                    if ("Before"u8.IsEqualConstant(state.StringBuffer))
                        return ForceRevisionStrategy.Before;

                    ThrowInvalidProperty(state, ctx);
                    break;

                default:
                    ThrowInvalidProperty(state, ctx);
                    break;
            }

            return 0;
        }

        private static unsafe AttachmentFlags GetAttachmentFlag(JsonParserState state, JsonOperationContext ctx)
        {
            // here we confirm that the value is matching our expectation, in order to save CPU instructions
            // we compare directly against the precomputed values
            switch (state.StringSize)
            {
                case 4:
                    if ("None"u8.IsEqualConstant(state.StringBuffer))
                        return AttachmentFlags.None;

                    ThrowInvalidProperty(state, ctx);
                    break;



                case 7:

                    if ("Retired"u8.IsEqualConstant(state.StringBuffer))
                        return AttachmentFlags.Retired;


                    ThrowInvalidProperty(state, ctx);
                    break;
                case 10:
                    if ("Compressed"u8.IsEqualConstant(state.StringBuffer))
                        return AttachmentFlags.Compressed;

                    ThrowInvalidProperty(state, ctx);
                    break;



                default:
                    ThrowInvalidProperty(state, ctx);
                    break;
            }

            return AttachmentFlags.None;
        }

        private static unsafe AttachmentType GetAttachmentType(JsonParserState state, JsonOperationContext ctx)
        {
            // here we confirm that the value is matching our expectation, in order to save CPU instructions
            // we compare directly against the precomputed values
            switch (state.StringSize)
            {
                case 8:
                    if ("Document"u8.IsEqualConstant(state.StringBuffer))
                        return AttachmentType.Document;

                    if ("Revision"u8.IsEqualConstant(state.StringBuffer))
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
                    if ("PUT"u8.IsEqualConstant(state.StringBuffer))
                        return CommandType.PUT;

                    break;

                case 5:
                    if ("PATCH"u8.IsEqualConstant(state.StringBuffer))
                        return CommandType.PATCH;

                    break;
                case 6:
                    if ("DELETE"u8.IsEqualConstant(state.StringBuffer))
                        return CommandType.DELETE;
                    break;

                case 8:
                    if ("Counters"u8.IsEqualConstant(state.StringBuffer))
                        return CommandType.Counters;
                    break;
                case 9:
                    if ("JsonPatch"u8.IsEqualConstant(state.StringBuffer))
                        return CommandType.JsonPatch;

                    if ("HeartBeat"u8.IsEqualConstant(state.StringBuffer))
                        return CommandType.HeartBeat;
                    break;
                case 10:
                    if ("TimeSeries"u8.IsEqualConstant(state.StringBuffer))
                        return CommandType.TimeSeries;

                    if ("BatchPATCH"u8.IsEqualConstant(state.StringBuffer))
                        return CommandType.BatchPATCH;
                    break;

                case 13:
                    if ("AttachmentPUT"u8.IsEqualConstant(state.StringBuffer))
                        return CommandType.AttachmentPUT;
                    break;

                case 14:
                    if ("TimeSeriesCopy"u8.IsEqualConstant(state.StringBuffer))
                        return CommandType.TimeSeriesCopy;

                    if ("AttachmentCOPY"u8.IsEqualConstant(state.StringBuffer))
                        return CommandType.AttachmentCOPY;

                    if ("AttachmentMOVE"u8.IsEqualConstant(state.StringBuffer))
                        return CommandType.AttachmentMOVE;

                    break;

                case 16:
                    if ("AttachmentDELETE"u8.IsEqualConstant(state.StringBuffer))
                        return CommandType.AttachmentDELETE;
                    break;

                case 18:
                    if ("CompareExchangePUT"u8.IsEqualConstant(state.StringBuffer))
                        return CommandType.CompareExchangePUT;
                    break;

                case 20:
                    if ("TimeSeriesBulkInsert"u8.IsEqualConstant(state.StringBuffer))
                        return CommandType.TimeSeriesBulkInsert;

                    ThrowInvalidProperty(state, ctx);
                    return CommandType.None;

                case 21:
                    if ("CompareExchangeDELETE"u8.IsEqualConstant(state.StringBuffer))
                        return CommandType.CompareExchangeDELETE;

                    if ("ForceRevisionCreation"u8.IsEqualConstant(state.StringBuffer))
                        return CommandType.ForceRevisionCreation;
                    break;

                case 24:
                    if ("TimeSeriesWithIncrements"u8.IsEqualConstant(state.StringBuffer))
                        return CommandType.TimeSeriesWithIncrements;
                    break;
            }

            ThrowInvalidCommandType(state, ctx);
            return CommandType.None;
        }

        [DoesNotReturn]
        private static unsafe void ThrowInvalidProperty(JsonParserState state, JsonOperationContext ctx)
        {
            throw new InvalidOperationException("Invalid property name: " +
                                                ctx.AllocateStringValue(null, state.StringBuffer, state.StringSize));
        }

        [DoesNotReturn]
        private static unsafe void ThrowInvalidCommandType(JsonParserState state, JsonOperationContext ctx)
        {
            throw new InvalidCommandTypeException("Invalid command type: "
                                                  + ctx.AllocateStringValue(null, state.StringBuffer, state.StringSize));
        }

        [DoesNotReturn]
        public static void ThrowUnexpectedToken(JsonParserToken jsonParserToken, JsonParserState state)
        {
            throw new InvalidOperationException("Expected " + jsonParserToken + " , but got " + state.CurrentTokenType);
        }


        public AbstractBatchCommandParsingObserver CommandParsingObserver { get; set; }

        public async Task RefillParserBuffer(Stream stream, JsonOperationContext.MemoryBuffer buffer, UnmanagedJsonParser parser, CancellationToken token = default)
        {
            CommandParsingObserver?.OnParserBufferRefill(parser);

            // Although we using here WithCancellation and passing the token,
            // the stream will stay open even after the cancellation until the entire server will be disposed.
            var read = await stream.ReadAsync(buffer.Memory.Memory, token).ConfigureAwait(false);
            if (read == 0)
                ThrowUnexpectedEndOfStream();
            parser.SetBuffer(buffer, 0, read);
        }

        [DoesNotReturn]
        private static void ThrowUnexpectedEndOfStream()
        {
            throw new EndOfStreamException();
        }
    }
}
