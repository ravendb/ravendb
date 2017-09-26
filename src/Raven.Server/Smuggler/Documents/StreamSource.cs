using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using Org.BouncyCastle.Cms;
using Raven.Client;
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
using Sparrow.Threading;
using Sparrow.Utils;
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

            while (type.Equals("Transformers", StringComparison.OrdinalIgnoreCase))
            {
                SkipArray();
                type = ReadType();
                if (type == null)
                    break;
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
                case DatabaseItemType.Tombstones:
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

        public IEnumerable<DocumentItem> GetLegacyAttachments(INewDocumentActions actions)
        {
            return ReadLegacyAttachments(actions);
        }

        public IEnumerable<DocumentTombstone> GetTombstones(List<string> collectionsToExport, INewDocumentActions actions)
        {
            return ReadTombstones(actions);
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

        public IDisposable GetIdentities(out IEnumerable<(string Prefix, long Value)> identities)
        {
            identities = InternalGetIdentities();
            return null;
        }

        private IEnumerable<(string Prefix, long Value)> InternalGetIdentities()
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

                    yield return (identityKey, identityValue);
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

        private IEnumerable<DocumentItem> ReadLegacyAttachments(INewDocumentActions actions)
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
                while (true)
                {
                    if (UnmanagedJsonParserHelper.Read(_stream, _parser, _state, _buffer) == false)
                        ThrowInvalidJson("Unexpected end of json while reading legacy attachments");

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

                    var attachment = new DocumentItem.AttachmentStream
                    {
                        Stream = actions.GetTempStream()
                    };

                    var attachmentInfo = ProcessLegacyAttachment(context, data, ref attachment);
                    var dummyDoc = new DocumentItem()
                    {
                        Document = new Document()
                        {
                            Data = WriteDummyDocumentForAttachment(context, attachmentInfo),//TODO:this is wrong i need to generate a dummy document just like in the DR tool
                            Id = attachmentInfo.Id,
                            ChangeVector = string.Empty,
                            Flags = DocumentFlags.HasAttachments,
                            NonPersistentFlags = NonPersistentDocumentFlags.FromSmuggler
                        }
                    };
                    dummyDoc.Attachments = new List<DocumentItem.AttachmentStream>();
                    dummyDoc.Attachments.Add(attachment);
                    yield return dummyDoc;
                }
            }
            finally
            {
                builder.Dispose();
            }

        }

        private BlittableJsonReaderObject WriteDummyDocumentForAttachment(DocumentsOperationContext context, LegacyAttachmentDetails details)
        {
            var attachment = new DynamicJsonValue
            {
                ["Name"] = details.Key,
                ["Hash"] = details.Hash,
                ["ContentType"] = string.Empty,
                ["size"] = details.Size,
            };
            var attachmets = new DynamicJsonArray();
            attachmets.Add(attachment);
            var metadata = new DynamicJsonValue
            {
                [Constants.Documents.Metadata.Collection] = "@empty",
                [Constants.Documents.Metadata.Attachments] = attachmets,
            };
            var djv = new DynamicJsonValue
            {
                [Constants.Documents.Metadata.Key] = metadata,

            };
            return context.ReadObject(djv, details.Id);
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

                    if (data.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata) &&
                        metadata.TryGet(DocumentItem.ExportDocumentType.Key, out string type) &&
                        type == DocumentItem.ExportDocumentType.Attachment)
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

        private IEnumerable<DocumentTombstone> ReadTombstones(INewDocumentActions actions = null)
        {
            if (UnmanagedJsonParserHelper.Read(_stream, _parser, _state, _buffer) == false)
                ThrowInvalidJson("Unexpected end of json");

            if (_state.CurrentTokenType != JsonParserToken.StartArray)
                ThrowInvalidJson("Expected start array, but got " + _state.CurrentTokenType);

            var context = _context;
            var builder = CreateBuilder(context, null);
            try
            {
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
                            builder = CreateBuilder(context, null);
                        }
                    }
                    builder.Renew("import/object", BlittableJsonDocumentBuilder.UsageMode.ToDisk);

                    ReadObject(builder);

                    var data = builder.CreateReader();
                    builder.Reset();

                    var tombstone = new DocumentTombstone();
                    if (data.TryGet("Key", out tombstone.LowerId) &&
                        data.TryGet(nameof(DocumentTombstone.Type), out string type) &&
                        data.TryGet(nameof(DocumentTombstone.Collection), out tombstone.Collection))
                    {
                        tombstone.Type = Enum.Parse<DocumentTombstone.TombstoneType>(type);
                        yield return tombstone;
                    }
                }
            }
            finally
            {
                builder.Dispose();
            }
        }

        internal unsafe LegacyAttachmentDetails ProcessLegacyAttachment(DocumentsOperationContext context, BlittableJsonReaderObject data, ref DocumentItem.AttachmentStream attachment)
        {
            BlittableJsonReaderObject bjr;
            string base64data;
            string key;
            if (data.TryGet("Metadata", out bjr) == false ||
                data.TryGet("Data", out base64data) == false ||
                data.TryGet("Key", out key) == false)
            {
                throw new ArgumentException($"Data of legacy attachment is not valid: {data}");
            }

            var memoryStream = new MemoryStream();

            fixed (char* pdata = base64data)
            {
                memoryStream.SetLength(Base64.FromBase64_ComputeResultLength(pdata, base64data.Length));
                fixed (byte* buffer = memoryStream.GetBuffer())
                    Base64.FromBase64_Decode(pdata, base64data.Length, buffer, (int)memoryStream.Length);
            }

            memoryStream.Position = 0;
            var stream = attachment.Stream;
            var hash = AsyncHelpers.RunSync(() => AttachmentsStorageHelper.CopyStreamToFileAndCalculateHash(context, memoryStream, stream, CancellationToken.None));
            var lazyHash = context.GetLazyString(hash);
            attachment.Base64HashDispose = Slice.External(context.Allocator, lazyHash, out attachment.Base64Hash);
            var tag = $"{_dummyDocument}{key}{_recordSeperator}d{_recordSeperator}{key}{_recordSeperator}{hash}{_recordSeperator}";
            var lazyTag = context.GetLazyString(tag);
            attachment.TagDispose = Slice.External(context.Allocator, lazyTag, out attachment.Tag);
            var id = $"{_dummyDocument}{key}";
            var lazyId = context.GetLazyString(id);

            attachment.Data = context.ReadObject(bjr, id); 
            return new LegacyAttachmentDetails
            {
                Id = lazyId,
                Hash = hash,
                Key = key,
                Size = attachment.Stream.Length,
                Tag = tag
            };
        }

        internal struct LegacyAttachmentDetails
        {
            public LazyStringValue Id;
            public string Hash;
            public string Key;
            public long Size;
            public string Tag;
        }

        private readonly char _recordSeperator = (char)SpecialChars.RecordSeparator;
        private readonly string _dummyDocument = "dummy/";

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

            if (type.Equals(nameof(DatabaseItemType.RevisionDocuments), StringComparison.OrdinalIgnoreCase))
                return DatabaseItemType.RevisionDocuments;

            if (type.Equals(nameof(DatabaseItemType.Tombstones), StringComparison.OrdinalIgnoreCase))
                return DatabaseItemType.Tombstones;

            if (type.Equals(nameof(DatabaseItemType.Indexes), StringComparison.OrdinalIgnoreCase))
                return DatabaseItemType.Indexes;

            if (type.Equals(nameof(DatabaseItemType.Identities), StringComparison.OrdinalIgnoreCase))
                return DatabaseItemType.Identities;
            if (type.Equals("Attachments", StringComparison.OrdinalIgnoreCase))
                return DatabaseItemType.LegacyAttachments;
            //Attachments
            throw new InvalidOperationException("Got unexpected property name '" + type + "' on " + _parser.GenerateErrorState());
        }
    }
}
