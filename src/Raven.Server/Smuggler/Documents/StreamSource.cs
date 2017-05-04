using System;
using System.Collections.Generic;
using System.IO;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Documents.Transformers;
using Raven.Client.Util;
using Raven.Server.Config.Settings;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;
using Raven.Server.Smuggler.Documents.Data;
using Raven.Server.Smuggler.Documents.Processors;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron;
using Size = Sparrow.Size;

namespace Raven.Server.Smuggler.Documents
{
    public class StreamSource : ISmugglerSource
    {
        private readonly Stream _stream;
        private readonly DocumentsOperationContext _context;
        private JsonOperationContext.ManagedPinnedBuffer _buffer;
        private JsonOperationContext.ReturnBuffer _returnBuffer;
        private JsonOperationContext.ManagedPinnedBuffer _writeBuffer;
        private JsonOperationContext.ReturnBuffer _returnWriteBuffer;
        private JsonParserState _state;
        private UnmanagedJsonParser _parser;
        private DatabaseItemType? _currentType;

        private SmugglerResult _result;

        private BuildVersionType _buildVersionType;

        private Size _totalObjectsRead = new Size(0, SizeUnit.Bytes);

        public StreamSource(Stream stream, DocumentsOperationContext context)
        {
            _stream = stream;
            _context = context;
        }

        public IDisposable Initialize(DatabaseSmugglerOptions options, SmugglerResult result, out long buildVersion)
        {
            _result = result;
            _returnBuffer = _context.GetManagedBuffer(out _buffer);
            _state = new JsonParserState();
            _parser = new UnmanagedJsonParser(_context, _state, "file");

            if (UnmanagedJsonParserHelper.Read(_stream, _parser, _state, _buffer) == false)
                ThrowInvalidJson();

            if (_state.CurrentTokenType != JsonParserToken.StartObject)
                ThrowInvalidJson();

            buildVersion = ReadBuildVersion();
            _buildVersionType = BuildVersion.Type(buildVersion);

            return new DisposableAction(() =>
            {
                _parser.Dispose();
                _returnBuffer.Dispose();
                _returnWriteBuffer.Dispose();
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
                        indexDefinition = IndexProcessor.ReadIndexDefinition(reader, _buildVersionType, out type);
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
                        transformerDefinition = TransformerProcessor.ReadTransformerDefinition(reader, _buildVersionType);
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
            var modifier = new BlittableMetadataModifier(context);
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
                    builder.Renew("import/object", BlittableJsonDocumentBuilder.UsageMode.ToDisk);

                    ReadObject(builder);

                    var data = builder.CreateReader();
                    builder.Reset();

                    if (data.TryGet(DocumentItem.Key, out byte type) &&
                        type == (byte)DocumentType.Attachment)
                    {
                        AddAttachmentStream(context, data, actions);
                        continue;
                    }

                    yield return new Document
                    {
                        Data = data,
                        Key = modifier.Id,
                        ChangeVector = modifier.ChangeVector,
                        Flags = modifier.Flags,
                        NonPersistentFlags = modifier.NonPersistentFlags,
                    };
                }
            }
            finally
            {
                builder.Dispose();
            }
        }

        public struct AttachmentStream : IDisposable
        {
            public Slice Base64Hash;
            public ByteStringContext<ByteStringMemoryCache>.ExternalScope Base64HashDispose;

            public FileStream File;
            public AttachmentsStorage.ReleaseTempFile FileDispose;

            public BlittableJsonReaderObject Data;

            public void Dispose()
            {
                Base64HashDispose.Dispose();
                FileDispose.Dispose();
                Data.Dispose();
            }
        }

        private unsafe void AddAttachmentStream(DocumentsOperationContext context, BlittableJsonReaderObject data, INewDocumentActions actions)
        {
            var documentActions = actions as DatabaseDestination.DatabaseDocumentActions;
            if (documentActions == null)
                return;

            if (data.TryGet(nameof(AttachmentResult.Hash), out LazyStringValue hash) == false ||
                data.TryGet(nameof(AttachmentResult.Size), out long size) == false)
                throw new ArgumentException($"Data of attachment stream is not valid: {data}");

            if (_writeBuffer == null)
                _returnWriteBuffer = _context.GetManagedBuffer(out _writeBuffer);

            var attachment = documentActions.CreateAttachment();
            attachment.Data = data;
            attachment.Base64HashDispose = Slice.External(context.Allocator, hash, out attachment.Base64Hash);

            while (size > 0)
            {
                var sizeToRead = (int)Math.Min(_writeBuffer.Length, size);
                var read = _parser.Copy(_writeBuffer.Pointer, sizeToRead);
                attachment.File.Write(_writeBuffer.Buffer.Array, _writeBuffer.Buffer.Offset, read.bytesRead);
                if (read.done == false)
                {
                    var read2 = _stream.Read(_buffer.Buffer.Array, _buffer.Buffer.Offset, _buffer.Length);
                    if (read2 == 0)
                        throw new EndOfStreamException("Stream ended without reaching end of stream content");

                    _parser.SetBuffer(_buffer, 0, read2);
                }
                size -= read.bytesRead;
            }
            attachment.File.Position = 0;

            documentActions.WriteAttachment(attachment);
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