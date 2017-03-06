using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Documents.Transformers;
using Raven.Client.Util;
using Raven.Server.Config.Settings;
using Raven.Server.Documents;
using Raven.Server.Smuggler.Documents.Data;
using Raven.Server.Smuggler.Documents.Processors;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Size = Raven.Server.Config.Settings.Size;

namespace Raven.Server.Smuggler.Documents
{
    public class StreamSource : ISmugglerSource
    {
        private static readonly StringSegment MetadataCollectionSegment = new StringSegment(Constants.Documents.Metadata.Collection );
        private readonly Stream _stream;
        private readonly JsonOperationContext _context;
        private JsonOperationContext.ManagedPinnedBuffer _buffer;
        private JsonOperationContext.ReturnBuffer _returnBuffer;
        private JsonParserState _state;
        private UnmanagedJsonParser _parser;
        private DatabaseItemType? _currentType;

        private DatabaseSmugglerOptions _options;
        private SmugglerResult _result;

        private long _buildVersion;

        private Size _totalObjectsRead = new Size(0, SizeUnit.Bytes);

        public StreamSource(Stream stream, JsonOperationContext context)
        {
            _stream = stream;
            _context = context;
        }

        public IDisposable Initialize(DatabaseSmugglerOptions options, SmugglerResult result, out long buildVersion)
        {
            _options = options;
            _result = result;
            _returnBuffer = _context.GetManagedBuffer(out _buffer);
            _state = new JsonParserState();
            _parser = new UnmanagedJsonParser(_context, _state, "file");

            if (UnmanagedJsonParserHelper.Read(_stream, _parser, _state, _buffer) == false)
                ThrowInvalidJson();

            if (_state.CurrentTokenType != JsonParserToken.StartObject)
                ThrowInvalidJson();

            _buildVersion = buildVersion = ReadBuildVersion();

            return new DisposableAction(() =>
            {
                _parser.Dispose();
                _returnBuffer.Dispose();
            });
        }

        public DatabaseItemType GetNextType()
        {
            if (_currentType != null)
            {
                var currentType = _currentType.Value;
                _currentType = null;

                return currentType;
            }

            var type = ReadType();
            if (type == null)
                return DatabaseItemType.None;

            if (type.Equals("Attachments", StringComparison.OrdinalIgnoreCase))
            {
                SkipArray();
                type = ReadType();
            }

            return GetType(type);
        }

        public long SkipType(DatabaseItemType type)
        {
            switch (type)
            {
                case DatabaseItemType.None:
                    return 0;
                case DatabaseItemType.Documents:
                case DatabaseItemType.RevisionDocuments:
                case DatabaseItemType.Indexes:
                case DatabaseItemType.Transformers:
                case DatabaseItemType.Identities:
                    return SkipArray();
                default:
                    throw new ArgumentOutOfRangeException(nameof(type), type, null);
            }
        }

        public class BlittableMetadataModifier : IDisposable, IBlittableDocumentModifier
        {
            private bool _readingMetadataObject;
            private int _depth;
            private State _state = State.None;
            private bool _verifyStartArray;

            public LazyStringValue Id;

            private JsonOperationContext _ctx;

            private readonly List<AllocatedMemoryData> _allocations = new List<AllocatedMemoryData>();

            private unsafe LazyStringValue CreateLazyStringValueFromParserState(JsonParserState state)
            {
                int escapePositionsCount = state.EscapePositions.Count;

                var maxSizeOfEscapePos = escapePositionsCount * 5 // max size of var int
                                         + JsonParserState.VariableSizeIntSize(escapePositionsCount);

                var mem = _ctx.GetMemory(maxSizeOfEscapePos + state.StringSize);
                _allocations.Add(mem);
                Memory.Copy(mem.Address, state.StringBuffer, state.StringSize);
                var lazyStringValueFromParserState = _ctx.AllocateStringValue(null, mem.Address, state.StringSize);
                if (escapePositionsCount > 0)
                {
                    lazyStringValueFromParserState.EscapePositions = state.EscapePositions.ToArray();
                }
                return lazyStringValueFromParserState;
            }

            private enum State
            {
                None,
                ReadingId,
                IgnorePropertyEtag,
                IgnoreProperty,
                IgnoreArray
            }

            public void StartObject()
            {
                if (_readingMetadataObject == false)
                    return;

                _depth++;
            }

            public void EndObject()
            {
                if (_readingMetadataObject == false)
                    return;

                _depth--;

                Debug.Assert(_depth >= 0);
                if (_depth == 0)
                    _readingMetadataObject = false;
            }

            public unsafe bool AboutToReadPropertyName(IJsonParser reader, JsonParserState state)
            {
                switch (_state)
                {
                    case State.None:
                        break;
                    case State.IgnoreProperty:
                        if (reader.Read() == false)
                            return false;
                        if (state.CurrentTokenType == JsonParserToken.StartArray ||
                            state.CurrentTokenType == JsonParserToken.StartObject)
                            ThrowInvalidMetadataProperty(state);
                        break;
                    case State.IgnorePropertyEtag:
                        if (reader.Read() == false)
                            return false;
                        if (state.CurrentTokenType != JsonParserToken.String &&
                            state.CurrentTokenType != JsonParserToken.Integer)
                            ThrowInvalidEtagType(state);
                        break;
                    case State.IgnoreArray:
                        if (_verifyStartArray)
                        {
                            if (reader.Read() == false)
                                return false;

                            _verifyStartArray = false;

                            if (state.CurrentTokenType != JsonParserToken.StartArray)
                                ThrowInvalidReplicationHistoryType(state);
                        }

                        while (state.CurrentTokenType != JsonParserToken.EndArray)
                        {
                            if (reader.Read() == false)
                                return false;
                        }
                        break;
                    case State.ReadingId:
                        if (reader.Read() == false)
                            return false;
                        if (state.CurrentTokenType != JsonParserToken.String)
                            ThrowInvalidIdType(state);
                        Id = CreateLazyStringValueFromParserState(state);
                        break;
                }

                _state = State.None;

                while (true)
                {
                    if (reader.Read() == false)
                        return false;

                    if (state.CurrentTokenType != JsonParserToken.String)
                        return true; // let the caller handle that

                    if (_readingMetadataObject == false)
                    {
                        if (state.StringSize == 9 && state.StringBuffer[0] == (byte)'@' &&
                            *(long*)(state.StringBuffer + 1) == 7022344802737087853)
                        {
                            _readingMetadataObject = true;
                        }
                        return true;
                    }

                    switch (state.StringSize)
                    {
                        case 3:// @id
                            if (state.StringBuffer[0] == (byte)'@' &&
                                *(short*)(state.StringBuffer + 1) == 25705)
                            {
                                if (reader.Read() == false)
                                {
                                    _state = State.ReadingId;
                                    return false;
                                }
                                if (state.CurrentTokenType != JsonParserToken.String)
                                    ThrowInvalidIdType(state);
                                Id = CreateLazyStringValueFromParserState(state);
                            }
                            break;
                        case 5:// @etag
                            if (state.StringBuffer[0] == (byte)'@' &&
                                *(int*)(state.StringBuffer + 1) == 1734440037)
                            {
                                if (reader.Read() == false)
                                {
                                    _state = State.IgnorePropertyEtag;
                                    return false;
                                }
                                if (state.CurrentTokenType != JsonParserToken.String &&
                                    state.CurrentTokenType != JsonParserToken.Integer)
                                    ThrowInvalidEtagType(state);
                            }
                            break;
                        case 13: //Last-Modified
                            if (*(long*)state.StringBuffer == 7237087983830262092 &&
                              *(int*)(state.StringBuffer + sizeof(long)) == 1701406313 &&
                              state.StringBuffer[12] == (byte)'d')
                            {
                                if (reader.Read() == false)
                                {
                                    _state = State.IgnoreProperty;
                                    return false;
                                }
                                if (state.CurrentTokenType == JsonParserToken.StartArray ||
                                    state.CurrentTokenType == JsonParserToken.StartObject)
                                    ThrowInvalidMetadataProperty(state);
                            }
                            break;
                        case 17: //Raven-Entity-Name --> @collection
                            if (*(long*)state.StringBuffer == 7945807069737017682 &&
                               *(long*)(state.StringBuffer + sizeof(long)) == 7881666780093245812)
                            {
                                var collection = _ctx.GetLazyStringForFieldWithCaching(MetadataCollectionSegment);
                                state.StringBuffer = collection.AllocatedMemoryData.Address;
                                state.StringSize = collection.Size;
                            }
                            return true;
                        case 19: //Raven-Last-Modified
                            if (*(long*)state.StringBuffer == 7011028672080929106 &&
                               *(long*)(state.StringBuffer + sizeof(long)) == 7379539893622240371 &&
                               *(short*)(state.StringBuffer + sizeof(long) + sizeof(long)) == 25961 &&
                               state.StringBuffer[18] == (byte)'d')
                            {
                                if (reader.Read() == false)
                                {
                                    _state = State.IgnoreProperty;
                                    return false;
                                }
                                if (state.CurrentTokenType == JsonParserToken.StartArray ||
                                    state.CurrentTokenType == JsonParserToken.StartObject)
                                    ThrowInvalidMetadataProperty(state);
                            }
                            break;
                        case 24: //Raven-Replication-Source
                            if (*(long*)state.StringBuffer == 7300947898092904786 &&
                               *(long*)(state.StringBuffer + sizeof(long)) == 8028075772393122928 &&
                               *(long*)(state.StringBuffer + sizeof(long) + sizeof(long)) == 7305808869229538670)
                            {
                                if (reader.Read() == false)
                                {
                                    _state = State.IgnoreProperty;
                                    return false;
                                }
                                if (state.CurrentTokenType == JsonParserToken.StartArray ||
                                    state.CurrentTokenType == JsonParserToken.StartObject)
                                    ThrowInvalidMetadataProperty(state);
                            }
                            break;
                        case 25: //Raven-Replication-Version OR Raven-Replication-History
                            if (*(long*)state.StringBuffer != 7300947898092904786 ||
                               *(long*)(state.StringBuffer + sizeof(long)) != 8028075772393122928)
                                break;

                            var value = *(long*)(state.StringBuffer + sizeof(long) + sizeof(long));
                            var lastByte = state.StringBuffer[24];
                            if (value == 8028074745928232302 && lastByte == (byte)'n' ||
                                value == 8245937481775066478 && lastByte == (byte)'y')
                            {
                                var isReplicationHistory = lastByte == (byte)'y';
                                if (reader.Read() == false)
                                {
                                    _verifyStartArray = isReplicationHistory;
                                    _state = isReplicationHistory ? State.IgnoreArray : State.IgnoreProperty;
                                    return false;
                                }

                                // Raven-Replication-History is an array
                                if (isReplicationHistory)
                                {
                                    if (state.CurrentTokenType != JsonParserToken.StartArray)
                                        ThrowInvalidReplicationHistoryType(state);

                                    do
                                    {
                                        if (reader.Read() == false)
                                        {
                                            _state = State.IgnoreArray;
                                            return false;
                                        }
                                    } while (state.CurrentTokenType != JsonParserToken.EndArray);
                                }
                                else if (state.CurrentTokenType == JsonParserToken.StartArray ||
                                    state.CurrentTokenType == JsonParserToken.StartObject)
                                    ThrowInvalidMetadataProperty(state);
                            }
                            break;
                        case 29: //Non-Authoritative-Information
                            if (*(long*)state.StringBuffer == 7526769800038477646 &&
                               *(long*)(state.StringBuffer + sizeof(long)) == 8532478930943832687 &&
                               *(long*)(state.StringBuffer + sizeof(long) + sizeof(long)) == 7886488383206796645 &&
                               *(int*)(state.StringBuffer + sizeof(long) + sizeof(long) + sizeof(long)) == 1869182049 &&
                               state.StringBuffer[28] == (byte)'n')
                            {
                                if (reader.Read() == false)
                                {
                                    _state = State.IgnoreProperty;
                                    return false;
                                }
                                if (state.CurrentTokenType == JsonParserToken.StartArray ||
                                    state.CurrentTokenType == JsonParserToken.StartObject)
                                    ThrowInvalidMetadataProperty(state);
                            }
                            break;
                        case 32: //Raven-Replication-Merged-History
                            if (*(long*)state.StringBuffer == 7300947898092904786 &&
                               *(long*)(state.StringBuffer + sizeof(long)) == 8028075772393122928 &&
                               *(long*)(state.StringBuffer + sizeof(long) + sizeof(long)) == 7234302117464059246 &&
                               *(long*)(state.StringBuffer + sizeof(long) + sizeof(long) + sizeof(long)) == 8751179571877464109)
                            {
                                if (reader.Read() == false)
                                {
                                    _state = State.IgnoreProperty;
                                    return false;
                                }
                                if (state.CurrentTokenType == JsonParserToken.StartArray ||
                                    state.CurrentTokenType == JsonParserToken.StartObject)
                                    ThrowInvalidMetadataProperty(state);
                            }
                            break;
                        default: // accept this property
                            return true;
                    }
                }
            }

            private static void ThrowInvalidMetadataProperty(JsonParserState state)
            {
                throw new InvalidDataException("Expected property @metadata to be a simpel type, but was " +
                                               state.CurrentTokenType);
            }

            private static void ThrowInvalidIdType(JsonParserState state)
            {
                throw new InvalidDataException(
                    $"Expected property @metadata.@id to have string type, but was: {state.CurrentTokenType}");
            }

            private static void ThrowInvalidEtagType(JsonParserState state)
            {
                throw new InvalidDataException("Expected property @metadata.@etag to have string or long type, but was: " +
                                               state.CurrentTokenType);
            }

            private static void ThrowInvalidReplicationHistoryType(JsonParserState state)
            {
                throw new InvalidDataException("Expected property @metadata.Raven-Replication-History to have array type, but was: " +
                                               state.CurrentTokenType);
            }


            public void Dispose()
            {
                for (int i = _allocations.Count - 1; i >= 0; i--)
                {
                    _ctx.ReturnMemory(_allocations[i]);
                }
                _allocations.Clear();
            }

            public void Reset(JsonOperationContext ctx)
            {
                if (_ctx == null)
                {
                    _ctx = ctx;
                    return;
                }
                Id = null;
                _depth = 0;
                _state = State.None;
                _readingMetadataObject = false;
                _ctx = ctx;

            }
        }

        public IEnumerable<Document> GetDocuments(List<string> collectionsToExport, INewDocumentActions actions)
        {
            return ReadDocuments(actions);
        }

        public IEnumerable<Document> GetRevisionDocuments(List<string> collectionsToExport, INewDocumentActions actions, int limit)
        {
            return ReadDocuments(actions);
        }

        public IEnumerable<IndexDefinitionAndType> GetIndexes()
        {
            foreach (var reader in ReadArray())
            {
                using (reader)
                {
                    IndexType type;
                    object indexDefinition;

                    try
                    {
                        indexDefinition = IndexProcessor.ReadIndexDefinition(reader, _buildVersion, out type);
                    }
                    catch (Exception e)
                    {
                        _result.Indexes.ErroredCount++;
                        _result.AddWarning($"Could not read index definition. Message: {e.Message}");

                        continue;
                    }

                    yield return new IndexDefinitionAndType
                    {
                        Type = type,
                        IndexDefinition = indexDefinition
                    };
                }
            }
        }

        public IEnumerable<TransformerDefinition> GetTransformers()
        {
            foreach (var reader in ReadArray())
            {
                using (reader)
                {
                    TransformerDefinition transformerDefinition;

                    try
                    {
                        transformerDefinition = TransformerProcessor.ReadTransformerDefinition(reader, _buildVersion);
                    }
                    catch (Exception e)
                    {
                        _result.Transformers.ErroredCount++;
                        _result.AddWarning($"Could not read transformer definition. Message: {e.Message}");

                        continue;
                    }

                    yield return transformerDefinition;
                }
            }
        }

        public IEnumerable<KeyValuePair<string, long>> GetIdentities()
        {
            foreach (var reader in ReadArray())
            {
                using (reader)
                {
                    string identityKey;
                    string identityValueString;
                    long identityValue;

                    if (reader.TryGet("Key", out identityKey) == false ||
                        reader.TryGet("Value", out identityValueString) == false ||
                        long.TryParse(identityValueString, out identityValue) == false)
                    {
                        _result.Identities.ErroredCount++;
                        _result.AddWarning("Could not read identity.");

                        continue;
                    }

                    yield return new KeyValuePair<string, long>(identityKey, identityValue);
                }
            }
        }

        private unsafe string ReadType()
        {
            if (UnmanagedJsonParserHelper.Read(_stream, _parser, _state, _buffer) == false)
                ThrowInvalidJson();

            if (_state.CurrentTokenType == JsonParserToken.EndObject)
                return null;

            if (_state.CurrentTokenType != JsonParserToken.String)
                ThrowInvalidJson();

            return _context.AllocateStringValue(null, _state.StringBuffer, _state.StringSize).ToString();
        }

        private static void ThrowInvalidJson()
        {
            throw new InvalidOperationException("Invalid JSON.");
        }

        private void ReadObject(BlittableJsonDocumentBuilder builder)
        {
            UnmanagedJsonParserHelper.ReadObject(builder, _stream, _parser, _buffer);

            _totalObjectsRead.Add(builder.SizeInBytes, SizeUnit.Bytes);
        }

        private long ReadBuildVersion()
        {
            var type = ReadType();
            if (type == null)
                return 0;

            if (type.Equals("BuildVersion", StringComparison.OrdinalIgnoreCase) == false)
            {
                _currentType = GetType(type);
                return 0;
            }

            if (UnmanagedJsonParserHelper.Read(_stream, _parser, _state, _buffer) == false)
                ThrowInvalidJson();

            if (_state.CurrentTokenType != JsonParserToken.Integer)
                ThrowInvalidJson();

            return _state.Long;
        }

        private long SkipArray()
        {
            var count = 0L;
            foreach (var builder in ReadArray())
            {
                count++; //skipping
            }

            return count;
        }

        private IEnumerable<BlittableJsonReaderObject> ReadArray(INewDocumentActions actions = null)
        {
            if (UnmanagedJsonParserHelper.Read(_stream, _parser, _state, _buffer) == false)
                ThrowInvalidJson();

            if (_state.CurrentTokenType != JsonParserToken.StartArray)
                ThrowInvalidJson();

            var context = _context;
            var builder = CreateBuilder(_context, null);
            try
            {
                while (true)
                {
                    if (UnmanagedJsonParserHelper.Read(_stream, _parser, _state, _buffer) == false)
                        ThrowInvalidJson();

                    if (_state.CurrentTokenType == JsonParserToken.EndArray)
                        break;
                    if (actions != null)
                    {
                        var oldContext = _context;
                        context = actions.GetContextForNewDocument();
                        if (_context != oldContext)
                        {
                            builder.Dispose();
                            builder = CreateBuilder(context, null);
                        }
                    }
                    builder.Renew("import/object", BlittableJsonDocumentBuilder.UsageMode.ToDisk); ;
                    ReadObject(builder);

                    var reader = builder.CreateReader();
                    builder.Reset();
                    yield return reader;
                }
            }
            finally
            {
                builder.Dispose();
            }
        }

        private IEnumerable<Document> ReadDocuments(INewDocumentActions actions = null)
        {
            if (UnmanagedJsonParserHelper.Read(_stream, _parser, _state, _buffer) == false)
                ThrowInvalidJson();

            if (_state.CurrentTokenType != JsonParserToken.StartArray)
                ThrowInvalidJson();

            var context = _context;
            var modifier = new BlittableMetadataModifier();
            var builder = CreateBuilder(context, modifier);
            try
            {
                while (true)
                {
                    if (UnmanagedJsonParserHelper.Read(_stream, _parser, _state, _buffer) == false)
                        ThrowInvalidJson();

                    if (_state.CurrentTokenType == JsonParserToken.EndArray)
                        break;

                    if (actions != null)
                    {
                        var oldContext = context;
                        context = actions.GetContextForNewDocument();
                        if (oldContext != context)
                        {
                            builder.Dispose();
                            builder = CreateBuilder(context, modifier);
                        }
                    }
                    modifier.Reset(context);
                    builder.Renew("import/object", BlittableJsonDocumentBuilder.UsageMode.ToDisk);

                    ReadObject(builder);

                    var blittableJsonReaderObject = builder.CreateReader();
                    builder.Reset();
                    yield return new Document
                    {
                        Data = blittableJsonReaderObject,
                        Key = modifier.Id,
                    };
                }
            }
            finally
            {
                builder.Dispose();
            }
        }

        private BlittableJsonDocumentBuilder CreateBuilder(JsonOperationContext context, BlittableMetadataModifier modifier)
        {
            return new BlittableJsonDocumentBuilder(context,
                BlittableJsonDocumentBuilder.UsageMode.ToDisk, "import/object", _parser, _state,
                modifier: modifier);
        }

        private static DatabaseItemType GetType(string type)
        {
            if (type == null)
                return DatabaseItemType.None;

            if (type.Equals("Docs", StringComparison.OrdinalIgnoreCase))
                return DatabaseItemType.Documents;

            if (type.Equals("RevisionDocuments", StringComparison.OrdinalIgnoreCase))
                return DatabaseItemType.RevisionDocuments;

            if (type.Equals("Indexes", StringComparison.OrdinalIgnoreCase))
                return DatabaseItemType.Indexes;

            if (type.Equals("Transformers", StringComparison.OrdinalIgnoreCase))
                return DatabaseItemType.Transformers;

            if (type.Equals("Identities", StringComparison.OrdinalIgnoreCase))
                return DatabaseItemType.Identities;

            throw new InvalidOperationException();
        }
    }
}