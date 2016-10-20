using System;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using Microsoft.AspNetCore.Http;
using Raven.Client.Data;
using Raven.Client.Data.Indexes;
using Raven.Client.Smuggler;
using Raven.Server.Documents;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Raven.Server.Smuggler.Documents.Data;
using Raven.Server.Smuggler.Documents.Processors;
using Sparrow.Json;

namespace Raven.Server.Smuggler.Documents
{
    public class SmugglerExporter
    {
        private readonly DocumentDatabase _database;

        public long? StartDocsEtag;
        public int? DocumentsLimit;

        public long? StartRevisionDocumentsEtag;
        public int? RevisionDocumentsLimit;

        public DatabaseExportOptions Options;
        public DatabaseItemType OperateOnTypes;

        public SmugglerExporter(DocumentDatabase database, DatabaseExportOptions options = null)
        {
            _database = database;
            OperateOnTypes = DatabaseItemType.Indexes | DatabaseItemType.Transformers
                | DatabaseItemType.Documents | DatabaseItemType.RevisionDocuments | DatabaseItemType.Identities;
            Options = options;
        }

        public ExportResult Export(DocumentsOperationContext context, string destinationFilePath, Action<IOperationProgress> onProgress = null)
        {
            using (var stream = File.Create(destinationFilePath))
            {
                return Export(context, stream, onProgress);
            }
        }

        public ExportResult Export(DocumentsOperationContext context, Stream destinationStream, Action<IOperationProgress> onProgress = null)
        {
            var result = new ExportResult();
            var progress = new IndeterminateProgress
            {
                Progress = "Starting Export",
            };

            onProgress?.Invoke(progress);
            using (var gZipStream = new GZipStream(destinationStream, CompressionMode.Compress, leaveOpen: true))
            using (var writer = new BlittableJsonTextWriter(context, gZipStream))
            {
                writer.WriteStartObject();

                writer.WritePropertyName(("BuildVersion"));
                writer.WriteInteger(40000);

                if (OperateOnTypes.HasFlag(DatabaseItemType.Documents))
                {
                    writer.WriteComma();
                    writer.WritePropertyName(("Docs"));

                    var documents = DocumentsLimit.HasValue
                        ? _database.DocumentsStorage.GetDocumentsFrom(context, StartDocsEtag ?? 0, 0, DocumentsLimit.Value)
                        : _database.DocumentsStorage.GetDocumentsFrom(context, StartDocsEtag ?? 0);
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
                }

                if (OperateOnTypes.HasFlag(DatabaseItemType.RevisionDocuments))
                {
                    var versioningStorage = _database.BundleLoader.VersioningStorage;
                    if (versioningStorage != null)
                    {
                        writer.WriteComma();
                        writer.WritePropertyName("RevisionDocuments");
                        writer.WriteStartArray();
                        var first = true;
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
                }

                if (OperateOnTypes.HasFlag(DatabaseItemType.Indexes))
                {
                    writer.WriteComma();
                    writer.WritePropertyName("Indexes");
                    writer.WriteStartArray();
                    var isFirst = true;
                    foreach (var index in _database.IndexStore.GetIndexes())
                    {
                        if (isFirst == false)
                            writer.WriteComma();

                        isFirst = false;

                        IndexProcessor.Export(writer, index, context);
                    }
                    writer.WriteEndArray();
                }

                if (OperateOnTypes.HasFlag(DatabaseItemType.Transformers))
                {
                    writer.WriteComma();
                    writer.WritePropertyName(("Transformers"));
                    writer.WriteStartArray();
                    var isFirst = true;
                    foreach (var transformer in _database.TransformerStore.GetTransformers())
                    {
                        if (isFirst == false)
                            writer.WriteComma();

                        isFirst = false;

                        TransformerProcessor.Export(writer, transformer, context);
                    }
                    writer.WriteEndArray();
                }

                if (OperateOnTypes.HasFlag(DatabaseItemType.Identities))
                {
                    writer.WriteComma();
                    writer.WritePropertyName(("Identities"));
                    writer.WriteStartArray();
                    var identities = _database.DocumentsStorage.GetIdentities(context);
                    var first = true;
                    foreach (var identity in identities)
                    {
                        if (first == false)
                            writer.WriteComma();
                        first = false;

                        writer.WriteStartObject();
                        writer.WritePropertyName(("Key"));
                        writer.WriteString((identity.Key));
                        writer.WriteComma();
                        writer.WritePropertyName(("Value"));
                        writer.WriteString((identity.Value.ToString()));
                        writer.WriteEndObject();
                    }
                    writer.WriteEndArray();
                }

                writer.WriteEndObject();
            }
            return result;
        }
    }
}