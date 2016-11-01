using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using Raven.Client.Data;
using Raven.Client.Smuggler;
using Raven.Server.Documents;
using Raven.Server.Documents.Patch;
using Raven.Server.ServerWide.Context;
using Raven.Server.Smuggler.Documents.Data;
using Raven.Server.Smuggler.Documents.Processors;
using Sparrow.Json;
using PatchRequest = Raven.Server.Documents.Patch.PatchRequest;

namespace Raven.Server.Smuggler.Documents
{
    public class SmugglerExporter
    {
        private readonly DocumentDatabase _database;
        public long? StartDocsEtag;
       
        public long? StartRevisionDocumentsEtag;

        public DatabaseSmugglerOptions Options;

        public SmugglerExporter(DocumentDatabase database, DatabaseSmugglerOptions options = null)
        {
            _database = database;
            Options = options ?? new DatabaseSmugglerOptions();
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
            var progress = new IndeterminateProgress();

            using (var gZipStream = new GZipStream(destinationStream, CompressionMode.Compress, leaveOpen: true))
            using (var writer = new BlittableJsonTextWriter(context, gZipStream))
            {
                writer.WriteStartObject();

                writer.WritePropertyName(("BuildVersion"));
                writer.WriteInteger(40000);

                if (Options.OperateOnTypes.HasFlag(DatabaseItemType.Documents))
                {
                    progress.Progress = "Exporting Documents";
                    onProgress?.Invoke(progress);
                    writer.WriteComma();
                    writer.WritePropertyName(("Docs"));

                    IEnumerable<Document> documents = Options.CollectionsToExport.Count != 0 ? 
                        _database.DocumentsStorage.GetDocumentsFrom(context, Options.CollectionsToExport, StartDocsEtag ?? 0, Options.BatchSize) : 
                        _database.DocumentsStorage.GetDocumentsFrom(context, StartDocsEtag ?? 0, 0, Options.BatchSize);

                    writer.WriteStartArray();

                    PatchDocument patch = null;
                    PatchRequest patchRequest = null;
                    if (string.IsNullOrWhiteSpace(Options.TransformScript) == false)
                    {
                        patch = new PatchDocument(context.DocumentDatabase);
                        patchRequest = new PatchRequest
                        {
                            Script = Options.TransformScript
                        };
                    }

                    bool first = true;
                    foreach (var document in documents)
                    {
                        if (document == null)
                            continue;

                        if (!Options.IncludeExpired && document.Expired())
                                continue;

                        if (patch != null)
                        {
                            var patchResult = patch.Apply(context, document, patchRequest);
                            if(patchResult == null || patchResult.ModifiedDocument.Equals(document.Data))
                                continue;
                            document.Data = patchResult.ModifiedDocument;
                        }

                        using (document.Data)
                        {
                            if (first == false)
                                writer.WriteComma();
                            first = false;

                            document.EnsureMetadata();
                            context.Write(writer, document.Data);
                            result.LastDocsEtag = document.Etag;
                        }
                        result.ExportedDocuments++;
                    }
                    writer.WriteEndArray();
                }
                
                if (Options.OperateOnTypes.HasFlag(DatabaseItemType.RevisionDocuments))
                {
                    var versioningStorage = _database.BundleLoader.VersioningStorage;
                    if (versioningStorage != null)
                    {
                        writer.WriteComma();
                        writer.WritePropertyName("RevisionDocuments");
                        writer.WriteStartArray();
                        var first = true;
                        var revisionDocuments = Options.RevisionDocumentsLimit.HasValue
                            ? versioningStorage.GetRevisionsAfter(context, StartRevisionDocumentsEtag ?? 0, Options.RevisionDocumentsLimit.Value)
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

                if (Options.OperateOnTypes.HasFlag(DatabaseItemType.Indexes))
                {
                    progress.Progress = "Exporting Indexes";
                    onProgress?.Invoke(progress);
                    writer.WriteComma();
                    writer.WritePropertyName("Indexes");
                    writer.WriteStartArray();
                    var isFirst = true;
                    foreach (var index in _database.IndexStore.GetIndexes())
                    {
                        if (isFirst == false)
                            writer.WriteComma();
                        isFirst = false;
                        IndexProcessor.Export(writer, index, context, Options.RemoveAnalyzers);
                    }
                    writer.WriteEndArray();
                }

                if (Options.OperateOnTypes.HasFlag(DatabaseItemType.Transformers))
                {
                    progress.Progress = "Exporting Transformers";
                    onProgress?.Invoke(progress);
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

                if (Options.OperateOnTypes.HasFlag(DatabaseItemType.Identities))
                {
                    progress.Progress = "Exporting Identities";
                    onProgress?.Invoke(progress);
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
                progress.Progress = $"Finish Exported database to {Options.FileName}. Exported {result.ExportedDocuments}";
                onProgress?.Invoke(progress);
            }
            return result;
        }
    }
}