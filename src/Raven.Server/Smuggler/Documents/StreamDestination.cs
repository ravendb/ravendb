using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Util;
using Raven.Server.Documents;
using Raven.Server.Documents.Indexes;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Raven.Server.Smuggler.Documents.Data;
using Sparrow.Json;

namespace Raven.Server.Smuggler.Documents
{
    public class StreamDestination : ISmugglerDestination
    {
        private readonly Stream _stream;
        private GZipStream _gzipStream;
        private readonly DocumentsOperationContext _context;
        private readonly DatabaseSource _source;
        private BlittableJsonTextWriter _writer;

        public StreamDestination(Stream stream, DocumentsOperationContext context, DatabaseSource source)
        {
            _stream = stream;
            _context = context;
            _source = source;
        }

        public IDisposable Initialize(DatabaseSmugglerOptions options, SmugglerResult result, long buildVersion)
        {
            _gzipStream = new GZipStream(_stream, CompressionMode.Compress, leaveOpen: true);
            _writer = new BlittableJsonTextWriter(_context, _gzipStream);

            _writer.WriteStartObject();

            _writer.WritePropertyName("BuildVersion");
            _writer.WriteInteger(buildVersion);

            return new DisposableAction(() =>
            {
                _writer.WriteEndObject();
                _writer.Dispose();
                _gzipStream.Dispose();
            });
        }

        public IDocumentActions Documents()
        {
            return new StreamDocumentActions(_writer, _context, _source, isRevision: false);
        }

        public IDocumentActions RevisionDocuments()
        {
            return new StreamDocumentActions(_writer, _context, _source, isRevision: true);
        }

        public IIdentityActions Identities()
        {
            return new StreamIdentityActions(_writer);
        }


        public IIndexActions Indexes()
        {
            return new StreamIndexActions(_writer, _context);
        }

        private class StreamIndexActions : StreamActionsBase, IIndexActions
        {
            private readonly JsonOperationContext _context;

            public StreamIndexActions(BlittableJsonTextWriter writer, JsonOperationContext context)
                : base(writer, "Indexes")
            {
                _context = context;
            }

            public void WriteIndex(IndexDefinitionBase indexDefinition, IndexType indexType)
            {
                if (First == false)
                    Writer.WriteComma();
                First = false;

                Writer.WriteStartObject();

                Writer.WritePropertyName(nameof(IndexDefinition.Type));
                Writer.WriteString(indexType.ToString());
                Writer.WriteComma();

                Writer.WritePropertyName(nameof(IndexDefinition));
                indexDefinition.Persist(_context, Writer);

                Writer.WriteEndObject();
            }

            public void WriteIndex(IndexDefinition indexDefinition)
            {
                if (First == false)
                    Writer.WriteComma();
                First = false;

                Writer.WriteStartObject();

                Writer.WritePropertyName(nameof(IndexDefinition.Type));
                Writer.WriteString(indexDefinition.Type.ToString());
                Writer.WriteComma();

                Writer.WritePropertyName(nameof(IndexDefinition));
                Writer.WriteIndexDefinition(_context, indexDefinition);

                Writer.WriteEndObject();
            }
        }

        private class StreamDocumentActions : StreamActionsBase, IDocumentActions
        {
            private readonly DocumentsOperationContext _context;
            private readonly DatabaseSource _source;
            private HashSet<string> _attachmentStreamsAlreadyExported;

            public StreamDocumentActions(BlittableJsonTextWriter writer, DocumentsOperationContext context, DatabaseSource source, bool isRevision)
                : base(writer, isRevision ? "RevisionDocuments" : "Docs")
            {
                _context = context;
                _source = source;
            }

            public void WriteDocument(DocumentItem item, SmugglerProgressBase.CountsWithLastEtag progress)
            {
                if (item.Attachments != null)
                    throw new NotSupportedException();

                var document = item.Document;
                using (document.Data)
                {
                    WriteUniqueAttachmentStreams(document, progress);

                    if (First == false)
                        Writer.WriteComma();
                    First = false;

                    document.EnsureMetadata();
                    _context.Write(Writer, document.Data);
                }
            }

            public Stream GetTempStream()
            {
                throw new NotSupportedException();
            }

            private void WriteUniqueAttachmentStreams(Document document, SmugglerProgressBase.CountsWithLastEtag progress)
            {
                if ((document.Flags & DocumentFlags.HasAttachments) != DocumentFlags.HasAttachments ||
                    document.Data.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata) == false ||
                    metadata.TryGet(Constants.Documents.Metadata.Attachments, out BlittableJsonReaderArray attachments) == false)
                    return;

                if (_attachmentStreamsAlreadyExported == null)
                    _attachmentStreamsAlreadyExported = new HashSet<string>();

                foreach (BlittableJsonReaderObject attachment in attachments)
                {
                    if (attachment.TryGet(nameof(AttachmentName.Hash), out LazyStringValue hash) == false)
                    {
                        progress.Attachments.ErroredCount++;

                        // TODO: How should we handle errors here?
                        throw new ArgumentException($"Hash field is mandatory in attachment's metadata: {attachment}");
                    }

                    progress.Attachments.ReadCount++;

                    if (_attachmentStreamsAlreadyExported.Add(hash))
                    {
                        using (var stream = _source.GetAttachmentStream(hash, out string tag))
                        {
                            WriteAttachmentStream(hash, stream, tag);
                        }
                    }
                }
            }

            public DocumentsOperationContext GetContextForNewDocument()
            {
                _context.CachedProperties.NewDocument();
                return _context;
            }

            private void WriteAttachmentStream(LazyStringValue hash, Stream stream, string tag)
            {
                if (First == false)
                    Writer.WriteComma();
                First = false;

                Writer.WriteStartObject();

                Writer.WritePropertyName(DocumentItem.Key);
                Writer.WriteInteger((byte)DocumentType.Attachment);
                Writer.WriteComma();

                Writer.WritePropertyName(nameof(AttachmentName.Hash));
                Writer.WriteString(hash);
                Writer.WriteComma();

                Writer.WritePropertyName(nameof(AttachmentName.Size));
                Writer.WriteInteger(stream.Length);
                Writer.WriteComma();

                Writer.WritePropertyName(nameof(DocumentItem.AttachmentStream.Tag));
                Writer.WriteString(tag);

                Writer.WriteEndObject();

                Writer.WriteStream(stream);
            }
        }        
        
        private class StreamIdentityActions : StreamActionsBase, IIdentityActions
        {
            public StreamIdentityActions(BlittableJsonTextWriter writer)
                : base(writer, "Identities")
            {
            }

            public void WriteIdentity(string key, long value)
            {
                if (First == false)
                    Writer.WriteComma();
                First = false;

                Writer.WriteStartObject();
                Writer.WritePropertyName("Key");
                Writer.WriteString(key);
                Writer.WriteComma();
                Writer.WritePropertyName("Value");
                Writer.WriteString(value.ToString());
                Writer.WriteEndObject();
            }
        }

        private abstract class StreamActionsBase : IDisposable
        {
            protected readonly BlittableJsonTextWriter Writer;

            protected bool First { get; set; }

            protected StreamActionsBase(BlittableJsonTextWriter writer, string propertyName)
            {
                Writer = writer;
                First = true;

                Writer.WriteComma();
                Writer.WritePropertyName(propertyName);
                Writer.WriteStartArray();
            }

            public void Dispose()
            {
                Writer.WriteEndArray();
            }
        }
    }
}
