using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Indexing;
using Raven.Client.Data.Indexes;
using Raven.Client.Smuggler;
using Raven.Server.Config.Settings;
using Raven.Server.Documents;
using Raven.Server.Smuggler.Documents.Data;
using Raven.Server.Smuggler.Documents.Processors;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Smuggler.Documents
{
    public class StreamSource : ISmugglerSource
    {
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

        private readonly Size _resetThreshold = new Size(32, SizeUnit.Megabytes);
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

        public IEnumerable<Document> GetDocuments(List<string> collectionsToExport, INewDocumentActions actions)
        {
            foreach (var data in ReadArray(actions))
            {
                BlittableJsonReaderObject metadata;
                if (data.TryGet(Constants.Metadata.Key, out metadata) == false || metadata == null)
                    ThrowInvalidJson();

                LazyStringValue id;
                Debug.Assert(metadata != null, "metadata != null");
                if (metadata.TryGet(Constants.Metadata.Id, out id) == false)
                    ThrowInvalidJson();

                yield return new Document
                {
                    Data = data,
                    Key = id
                };
            }
        }

        public IEnumerable<Document> GetRevisionDocuments(List<string> collectionsToExport, INewDocumentActions actions, int limit)
        {
            foreach (var data in ReadArray(actions))
            {
                BlittableJsonReaderObject metadata;
                if (data.TryGet(Constants.Metadata.Key, out metadata) == false || metadata == null)
                    ThrowInvalidJson();
                Debug.Assert(metadata != null);
                LazyStringValue id;
                if (metadata.TryGet(Constants.Metadata.Id, out id) == false)
                    ThrowInvalidJson();

                yield return new Document
                {
                    Data = data,
                    Key = id
                };
            }
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

            return new LazyStringValue(null, _state.StringBuffer, _state.StringSize, _context).ToString();
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

            while (true)
            {
                MaybeResetContextAndParser();
                if (UnmanagedJsonParserHelper.Read(_stream, _parser, _state, _buffer) == false)
                    ThrowInvalidJson();

                if (_state.CurrentTokenType == JsonParserToken.EndArray)
                    break;

                var context = actions == null ? _context : actions.GetContextForNewDocument();
                using (
                    var builder = new BlittableJsonDocumentBuilder(context,
                        BlittableJsonDocumentBuilder.UsageMode.ToDisk, "import/object", _parser, _state))
                {

                    ReadObject(builder);

                    yield return builder.CreateReader();
                }

            }
        }

        private void MaybeResetContextAndParser()
        {
            if (_totalObjectsRead < _resetThreshold)
                return;

            _totalObjectsRead = new Size(0, SizeUnit.Bytes);
            _parser.ResetStream();
            _parser.SetStream();
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