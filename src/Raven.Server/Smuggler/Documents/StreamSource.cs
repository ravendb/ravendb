using System;
using System.Collections.Generic;
using System.IO;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Util;
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
                ThrowInvalidJson("Unexpected end of json.");

            if (_state.CurrentTokenType != JsonParserToken.StartObject)
                ThrowInvalidJson("Expected start object, but got " + _state.CurrentTokenType);

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
                case DatabaseItemType.Identities:
                    return SkipArray();
                default:
                    throw new ArgumentOutOfRangeException(nameof(type), type, null);
            }
        }

        public IEnumerable<DocumentItem> GetDocuments(List<string> collectionsToExport, INewDocumentActions actions)
        {
            return ReadDocuments(actions);
        }

        public IEnumerable<DocumentItem> GetRevisionDocuments(List<string> collectionsToExport, INewDocumentActions actions)
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

        public IEnumerable<KeyValuePair<string, long>> GetIdentities()
        {
            return InternalGetIdentities();
        }

        private IEnumerable<KeyValuePair<string, long>> InternalGetIdentities()
        {
            foreach (var reader in ReadArray())
            {
                using (reader)
                {
                    if (reader.TryGet("Key", out string identityKey) == false ||
                        reader.TryGet("Value", out string identityValueString) == false ||
                        long.TryParse(identityValueString, out long identityValue) == false)
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
                ThrowInvalidJson("Unexpected end of object when reading type");

            if (_state.CurrentTokenType == JsonParserToken.EndObject)
                return null;

            if (_state.CurrentTokenType != JsonParserToken.String)
                ThrowInvalidJson("Expected property type to be string, but was " + _state.CurrentTokenType);

            return _context.AllocateStringValue(null, _state.StringBuffer, _state.StringSize).ToString();
        }

        private void ThrowInvalidJson(string msg)
        {
            throw new InvalidOperationException("Invalid JSON. " + msg + " on " + _parser.GenerateErrorState());
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
                ThrowInvalidJson("Unexpected end of json.");

            if (_state.CurrentTokenType != JsonParserToken.Integer)
                ThrowInvalidJson("Expected integer BuildVersion, but got " + _state.CurrentTokenType);

            return _state.Long;
        }

        private long SkipArray()
        {
            var count = 0L;
            foreach (var _ in ReadArray())
            {
                count++; //skipping
            }

            return count;
        }

        private IEnumerable<BlittableJsonReaderObject> ReadArray(INewDocumentActions actions = null)
        {
            if (UnmanagedJsonParserHelper.Read(_stream, _parser, _state, _buffer) == false)
                ThrowInvalidJson("Unexpected end of json");

            if (_state.CurrentTokenType != JsonParserToken.StartArray)
                ThrowInvalidJson("Expected start array, got " + _state.CurrentTokenType);

            var builder = CreateBuilder(_context, null);
            try
            {
                while (true)
                {
                    if (UnmanagedJsonParserHelper.Read(_stream, _parser, _state, _buffer) == false)
                        ThrowInvalidJson("Unexpected end of json while reading array");

                    if (_state.CurrentTokenType == JsonParserToken.EndArray)
                        break;
                    if (actions != null)
                    {
                        var oldContext = _context;
                        var context = actions.GetContextForNewDocument();
                        if (_context != oldContext)
                        {
                            builder.Dispose();
                            builder = CreateBuilder(context, null);
                        }
                    }
                    builder.Renew("import/object", BlittableJsonDocumentBuilder.UsageMode.ToDisk);
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

        private IEnumerable<DocumentItem> ReadDocuments(INewDocumentActions actions = null)
        {
            if (UnmanagedJsonParserHelper.Read(_stream, _parser, _state, _buffer) == false)
                ThrowInvalidJson("Unexpected end of json");

            if (_state.CurrentTokenType != JsonParserToken.StartArray)
                ThrowInvalidJson("Expected start array, but got " + _state.CurrentTokenType);

            var context = _context;
            var modifier = new BlittableMetadataModifier(context);
            var builder = CreateBuilder(context, modifier);
            try
            {
                List<DocumentItem.AttachmentStream> attachments = null;
                while (true)
                {
                    if (UnmanagedJsonParserHelper.Read(_stream, _parser, _state, _buffer) == false)
                        ThrowInvalidJson("Unexpected end of json while reading docs");

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
                        if (attachments == null)
                            attachments = new List<DocumentItem.AttachmentStream>();

                        var attachment = new DocumentItem.AttachmentStream
                        {
                            Stream = actions.GetTempStream()
                        };
                        ProcessAttachmentStream(context, data, ref attachment);
                        attachments.Add(attachment);
                        continue;
                    }

                    yield return new DocumentItem
                    {
                        Document = new Document
                        {
                            Data = data,
                            Id = modifier.Id,
                            ChangeVector = modifier.ChangeVector,
                            Flags = modifier.Flags,
                            NonPersistentFlags = modifier.NonPersistentFlags
                        },
                        Attachments = attachments
                    };
                    attachments = null;
                }
            }
            finally
            {
                builder.Dispose();
            }
        }

        public unsafe void ProcessAttachmentStream(DocumentsOperationContext context, BlittableJsonReaderObject data, ref DocumentItem.AttachmentStream attachment)
        {
            if (data.TryGet(nameof(AttachmentName.Hash), out LazyStringValue hash) == false ||
                data.TryGet(nameof(AttachmentName.Size), out long size) == false ||
                data.TryGet(nameof(DocumentItem.AttachmentStream.Tag), out LazyStringValue tag) == false)
                throw new ArgumentException($"Data of attachment stream is not valid: {data}");

            if (_writeBuffer == null)
                _returnWriteBuffer = _context.GetManagedBuffer(out _writeBuffer);

            attachment.Data = data;
            attachment.Base64HashDispose = Slice.External(context.Allocator, hash, out attachment.Base64Hash);
            attachment.TagDispose = Slice.External(context.Allocator, tag, out attachment.Tag);

            while (size > 0)
            {
                var sizeToRead = (int)Math.Min(_writeBuffer.Length, size);
                var read = _parser.Copy(_writeBuffer.Pointer, sizeToRead);
                attachment.Stream.Write(_writeBuffer.Buffer.Array, _writeBuffer.Buffer.Offset, read.bytesRead);
                if (read.done == false)
                {
                    var read2 = _stream.Read(_buffer.Buffer.Array, _buffer.Buffer.Offset, _buffer.Length);
                    if (read2 == 0)
                        throw new EndOfStreamException("Stream ended without reaching end of stream content");

                    _parser.SetBuffer(_buffer, 0, read2);
                }
                size -= read.bytesRead;
            }
        }

        private BlittableJsonDocumentBuilder CreateBuilder(JsonOperationContext context, BlittableMetadataModifier modifier)
        {
            return new BlittableJsonDocumentBuilder(context,
                BlittableJsonDocumentBuilder.UsageMode.ToDisk, "import/object", _parser, _state,
                modifier: modifier);
        }

        private DatabaseItemType GetType(string type)
        {
            if (type == null)
                return DatabaseItemType.None;

            if (type.Equals("Docs", StringComparison.OrdinalIgnoreCase))
                return DatabaseItemType.Documents;

            if (type.Equals("RevisionDocuments", StringComparison.OrdinalIgnoreCase))
                return DatabaseItemType.RevisionDocuments;

            if (type.Equals("Indexes", StringComparison.OrdinalIgnoreCase))
                return DatabaseItemType.Indexes;

            if (type.Equals("Identities", StringComparison.OrdinalIgnoreCase))
                return DatabaseItemType.Identities;

            throw new InvalidOperationException("Got unexpected property name '" + type + "' on " + _parser.GenerateErrorState());
        }
    }
}
