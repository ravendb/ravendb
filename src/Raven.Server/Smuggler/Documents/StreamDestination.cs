using System;
using System.IO;
using System.IO.Compression;
using Raven.Client.Data.Indexes;
using Raven.Client.Extensions;
using Raven.Client.Indexing;
using Raven.Client.Smuggler;
using Raven.Server.Documents;
using Raven.Server.Documents.Indexes;
using Raven.Server.Json;
using Raven.Server.Smuggler.Documents.Data;
using Sparrow.Json;

namespace Raven.Server.Smuggler.Documents
{
    public class StreamDestination : ISmugglerDestination
    {
        private readonly Stream _stream;
        private GZipStream _gzipStream;
        private readonly JsonOperationContext _context;
        private BlittableJsonTextWriter _writer;

        public StreamDestination(Stream stream, JsonOperationContext context)
        {
            _stream = stream;
            _context = context;
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
            return new StreamDocumentActions(_writer, _context, isRevision: false);
        }

        public IDocumentActions RevisionDocuments()
        {
            return new StreamDocumentActions(_writer, _context, isRevision: true);
        }

        public IIdentityActions Identities()
        {
            return new StreamIdentityActions(_writer);
        }

        public IIndexActions Indexes()
        {
            return new StreamIndexActions(_writer, _context);
        }

        public ITransformerActions Transformers()
        {
            return new StreamTransformerActions(_writer, _context);
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

        private class StreamTransformerActions : StreamActionsBase, ITransformerActions
        {
            private readonly JsonOperationContext _context;

            public StreamTransformerActions(BlittableJsonTextWriter writer, JsonOperationContext context)
                : base(writer, "Transformers")
            {
                _context = context;
            }

            public void WriteTransformer(TransformerDefinition transformerDefinition)
            {
                if (First == false)
                    Writer.WriteComma();
                First = false;

                Writer.WriteTransformerDefinition(_context, transformerDefinition);
            }
        }

        private class StreamDocumentActions : StreamActionsBase, IDocumentActions
        {
            private readonly JsonOperationContext _context;

            public StreamDocumentActions(BlittableJsonTextWriter writer, JsonOperationContext context, bool isRevision)
                : base(writer, isRevision ? "RevisionDocuments" : "Docs")
            {
                _context = context;
            }

            public void WriteDocument(Document document)
            {
                using (document.Data)
                {
                    if (First == false)
                        Writer.WriteComma();
                    First = false;

                    document.EnsureMetadata();
                    _context.Write(Writer, document.Data);
                }
            }

            public JsonOperationContext GetContextForNewDocument()
            {
                _context.CachedProperties.NewDocument();
                return _context;
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