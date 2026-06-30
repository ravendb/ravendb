using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Threading.Tasks;
using Raven.Server.Documents.Replication.Senders;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Json;
using Sparrow.Utils;

namespace Raven.Server.Documents.Handlers.Debugging
{
    public sealed class DocumentDebugHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/debug/documents/huge", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, IsDebugInformationEndpoint = true)]
        public async Task HugeDocuments()
        {
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriterForDebug(context, ServerStore, ResponseBodyStream()))
            using (context.OpenReadTransaction())
            {
                writer.WriteStartObject();
                writer.WritePropertyName("Results");

                writer.WriteStartArray();

                var isFirst = true;

                foreach (var pair in context.DocumentDatabase.HugeDocuments.GetHugeDocuments())
                {
                    if (isFirst == false)
                        writer.WriteComma();

                    isFirst = false;

                    writer.WriteStartObject();

                    writer.WritePropertyName(nameof(HugeDocumentInfo.Id));
                    writer.WriteString(pair.Key.Item1);

                    writer.WriteComma();

                    writer.WritePropertyName(nameof(HugeDocumentInfo.Size));
                    ;
                    writer.WriteInteger(pair.Value);

                    writer.WriteComma();

                    writer.WritePropertyName(nameof(HugeDocumentInfo.LastAccess));
                    writer.WriteString(pair.Key.Item2.ToString(DefaultFormat.DateTimeOffsetFormatsToWrite, CultureInfo.InvariantCulture));

                    writer.WriteEndObject();
                }

                writer.WriteEndArray();

                writer.WriteEndObject();
            }
        }

        internal class HugeDocumentInfo
        {
            public string Id { get; set; }

            public long Size { get; set; }

            public string LastAccess { get; set; }
        }

        [RavenAction("/databases/*/debug/documents/scan-corrupted-ids", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task ScanCorruptedIds()
        {
            var startEtag = GetLongQueryString("startEtag", required: false) ?? 0;
            var resultCount = GetIntValueQueryString("resultCount", required: false) ?? 1024;
            var maxBatchTimeInSec = GetIntValueQueryString("maxBatchTimeInSec", required: false) ?? 60;

            var wrongEscapedPositionsIds = new List<string>();
            var unescapedControlCharacterIds = new List<string>();
            long lastEtag = startEtag;
            var scannedDocuments = 0;
            var maxBatchTime = TimeSpan.FromSeconds(maxBatchTimeInSec);

            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                bool hasMore = true;
                
                await using (var writer = new AsyncBlittableJsonTextWriter(context, Stream.Null))
                {
                    var timeout = Stopwatch.StartNew();
                    while (resultCount > 0 && hasMore)
                    {
                        timeout.Restart();
                        using (context.OpenReadTransaction())
                        using (var enumerator = IterateDocumentsAndRevisionsByEtag(context, lastEtag).GetEnumerator())
                        {
                            while (true)
                            {
                                HttpContext.RequestAborted.ThrowIfCancellationRequested();
                                
                                hasMore = enumerator.MoveNext();
                                if (hasMore == false)
                                    break;
                                    
                                var doc = enumerator.Current;
                                using (doc)
                                {
                                    unsafe
                                    {
                                        StringUtils.FindMaxEscapePositionAndControlCharSizeForBackwardCompatibility(doc.LowerId.Buffer, doc.LowerId.Size, out var count);
                                        if (count > 0)
                                        {
                                            AddToResult(unescapedControlCharacterIds, doc);
                                            resultCount--;
                                        }
                                    }

                                    try
                                    {
                                        writer.WriteString(doc.Id);
                                    }
                                    catch (Exception)
                                    {
                                        AddToResult(wrongEscapedPositionsIds, doc);
                                        resultCount--;
                                    }
                                    scannedDocuments++;
                                    if (resultCount <= 0 || timeout.Elapsed > maxBatchTime)
                                    {
                                        lastEtag = doc.Etag;
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }

                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartObject();
                    writer.WriteArray("WrongEscapedPositionsIds", wrongEscapedPositionsIds);
                    writer.WriteComma();
                    writer.WriteArray("UnescapedControlCharacterIds", unescapedControlCharacterIds);
                    writer.WriteComma();
                    if (hasMore)
                    {
                        writer.WritePropertyName("LastEtag");
                        writer.WriteInteger(lastEtag);
                    }
                    else
                    {
                        writer.WritePropertyName("AllScanned");
                        writer.WriteBool(true);
                    }

                    writer.WriteComma();
                    writer.WritePropertyName("ScannedDocuments");
                    writer.WriteInteger(scannedDocuments);
                    writer.WriteEndObject();
                }
            }

            return;

            void AddToResult(List<string> list, Document doc)
            {
                list.Add(
                    $"Id: '{doc.Id}', LowerId: '{doc.LowerId}', ChangeVector: '{doc.ChangeVector}', Etag: '{doc.Etag}', Flags: '{doc.Flags}'");
            }
        }

        private IEnumerable<Document> IterateDocumentsAndRevisionsByEtag(DocumentsOperationContext context, long startEtag)
        {
            const DocumentFields documentFields = DocumentFields.Id | DocumentFields.LowerId | DocumentFields.ChangeVector;
            var documents = Database.DocumentsStorage.GetDocumentsFrom(context, startEtag, 0, long.MaxValue, documentFields);
            var revisions = Database.DocumentsStorage.RevisionsStorage.GetRevisionsFrom(context, startEtag, long.MaxValue, documentFields);

            using var documentsEnumerator = documents.GetEnumerator();
            using var revisionsEnumerator = revisions.GetEnumerator();

            var mergedEnumerator = new MergedEnumerator<Document>(DocumentsEtagComparer.Instance);
            mergedEnumerator.AddEnumerator(documentsEnumerator);
            mergedEnumerator.AddEnumerator(revisionsEnumerator);

            while (mergedEnumerator.MoveNext())
            {
                yield return mergedEnumerator.Current;
            }
        }

        private class DocumentsEtagComparer : IComparer<Document>
        {
            public static readonly DocumentsEtagComparer Instance = new();

            private DocumentsEtagComparer()
            {
            }

            public int Compare(Document x, Document y) => x.Etag.CompareTo(y.Etag);
        }
    }
}
