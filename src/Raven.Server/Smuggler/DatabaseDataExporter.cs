using System.IO;
using System.IO.Compression;
using Raven.Client.Data.Indexes;
using Raven.Server.Documents;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Smuggler
{
    public class DatabaseDataExporter
    {
        private readonly DocumentDatabase _database;

        public long? StartDocsEtag;
        public int? DocumentsLimit;

        public long? StartRevisionDocumentsEtag;
        public int? RevisionDocumentsLimit;

        public DatabaseDataExporter(DocumentDatabase database)
        {
            _database = database;
        }

        public ExportResult Export(DocumentsOperationContext context, string destinationFilePath)
        {
            using (var stream = File.Create(destinationFilePath))
            {
                return Export(context, stream);
            }
        }

        public ExportResult Export(DocumentsOperationContext context, Stream destinationStream)
        {
            var result = new ExportResult();

            using (var gZipStream = new GZipStream(destinationStream, CompressionMode.Compress, leaveOpen: true))
            using (var writer = new BlittableJsonTextWriter(context, gZipStream))
            {
                writer.WriteStartObject();

                writer.WritePropertyName(context.GetLazyString("BuildVersion"));
                writer.WriteInteger(40000);

                writer.WriteComma();
                writer.WritePropertyName(context.GetLazyString("Docs"));
                var documents = DocumentsLimit.HasValue
                      ? _database.DocumentsStorage.GetDocumentsAfter(context, StartDocsEtag ?? 0, 0, DocumentsLimit.Value)
                      : _database.DocumentsStorage.GetDocumentsAfter(context, StartDocsEtag ?? 0);
                writer.WriteStartArray();
                bool first = true;
                foreach (var document in documents)
                {
                    if (document == null)
                        continue;

                    using (document.Data)
                    {
                        if (first == false)
                            writer.WriteComma();
                        first = false;

                        document.EnsureMetadata();
                        context.Write(writer, document.Data);
                        result.LastDocsEtag = document.Etag;
                    }
                }
                writer.WriteEndArray();

                var versioningStorage = _database.BundleLoader.VersioningStorage;
                if (versioningStorage != null)
                {
                    writer.WriteComma();
                    writer.WritePropertyName(context.GetLazyString("RevisionDocuments"));
                    writer.WriteStartArray();
                    first = true;
                    var revisionDocuments = RevisionDocumentsLimit.HasValue 
                        ? versioningStorage.GetRevisionsAfter(context, StartRevisionDocumentsEtag ?? 0, RevisionDocumentsLimit.Value)
                        : versioningStorage.GetRevisionsAfter(context, StartRevisionDocumentsEtag ?? 0);
                    foreach (var revisionDocument in revisionDocuments)
                    {
                        if (revisionDocument == null)
                            continue;

                        using (revisionDocument.Data)
                        {
                            if (first == false)
                                writer.WriteComma();
                            first = false;

                            revisionDocument.EnsureMetadata();
                            context.Write(writer, revisionDocument.Data);
                            result.LastRevisionDocumentsEtag = revisionDocument.Etag;
                        }
                    }
                    writer.WriteEndArray();
                }

                writer.WriteComma();
                writer.WritePropertyName(context.GetLazyString("Indexes"));
                writer.WriteStartArray();
                foreach (var index in _database.IndexStore.GetIndexes())
                {
                    if (index.Type == IndexType.Map || index.Type == IndexType.MapReduce)
                    {
                        var indexDefinition = index.GetIndexDefinition();
                        writer.WriteIndexDefinition(context, indexDefinition);
                    }
                    else if (index.Type == IndexType.Faulty)
                    {
                        // TODO: Should we export them?
                    }
                    else
                    {
                        // TODO: Export auto indexes.
                    }
                }
                writer.WriteEndArray();

                writer.WriteComma();
                writer.WritePropertyName(context.GetLazyString("Transformers"));
                writer.WriteStartArray();
                foreach (var transformer in _database.TransformerStore.GetTransformers())
                {
                    writer.WriteTransformerDefinition(context, transformer.Definition);
                }
                writer.WriteEndArray();

                writer.WriteComma();
                writer.WritePropertyName(context.GetLazyString("Identities"));
                writer.WriteStartArray();
                var identities = _database.DocumentsStorage.GetIdentities(context);
                first = true;
                foreach (var identity in identities)
                {
                    if (first == false)
                        writer.WriteComma();
                    first = false;

                    writer.WriteStartObject();
                    writer.WritePropertyName(context.GetLazyString("Key"));
                    writer.WriteString(context.GetLazyString(identity.Key));
                    writer.WriteComma();
                    writer.WritePropertyName(context.GetLazyString("Value"));
                    writer.WriteString(context.GetLazyString(identity.Value.ToString()));
                    writer.WriteEndObject();
                }
                writer.WriteEndArray();

                writer.WriteEndObject();
            }

            return result;
        }
    }
}