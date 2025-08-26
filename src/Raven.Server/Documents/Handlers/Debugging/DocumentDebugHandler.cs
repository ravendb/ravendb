using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Threading.Tasks;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Debugging
{
    public class DocumentDebugHandler : DatabaseRequestHandler
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

                    writer.WritePropertyName("Id");
                    writer.WriteString(pair.Key.Item1);

                    writer.WriteComma();

                    writer.WritePropertyName("Size");
                    writer.WriteInteger(pair.Value);

                    writer.WriteComma();

                    writer.WritePropertyName("LastAccess");
                    writer.WriteString(pair.Key.Item2.ToString(DefaultFormat.DateTimeOffsetFormatsToWrite, CultureInfo.InvariantCulture));

                    writer.WriteEndObject();
                }

                writer.WriteEndArray();

                writer.WriteEndObject();
            }
        }
        
        [RavenAction("/databases/*/debug/documents/scan-corrupted-ids", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task ScanCorruptedIds()
        {
            var startEtag = GetIntValueQueryString("startEtag", required: false) ?? 0;
            var corruptedCount = GetIntValueQueryString("corruptedCount", required: false) ?? 1024;
            
            var corrupted = new List<string>();
            long? lastEtag = null;
            
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                await using (var writer = new AsyncBlittableJsonTextWriter(context, Stream.Null))
                using (context.OpenReadTransaction())
                {
                    foreach (var doc in IterateDocumentsAndRevisionsByEtag(context, startEtag))
                    {
                        HttpContext.RequestAborted.ThrowIfCancellationRequested();
                        
                        using (doc)
                        {
                            try
                            {
                                writer.WriteString(doc.Id);
                            }
                            catch (Exception)
                            {
                                corrupted.Add($"Id: '{doc.Id}', LowerId: '{doc.LowerId}', ChangeVector: '{doc.ChangeVector}', Etag: '{doc.Etag}', Flags: '{doc.Flags}'");

                                if (corruptedCount-- <= 0)
                                {
                                    lastEtag = doc.Etag;
                                    break;
                                }
                            }
                        }
                    }
                }
            
                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartObject();
                    writer.WriteArray("CorruptedDocuments", corrupted);
                    writer.WriteComma();
                    if (lastEtag.HasValue)
                    {
                        writer.WritePropertyName("LastEtag");
                        writer.WriteInteger(lastEtag.Value);
                    }
                    else
                    {
                        writer.WritePropertyName("AllScanned");
                        writer.WriteBool(true);
                    }
                    writer.WriteEndObject();
                }
            }
        }
        
        public IEnumerable<Document> IterateDocumentsAndRevisionsByEtag(DocumentsOperationContext context, long startEtag)
        {
            const DocumentFields documentFields = DocumentFields.Id | DocumentFields.LowerId | DocumentFields.ChangeVector;
            var documents = Database.DocumentsStorage.GetDocumentsFrom(context, startEtag, 0, long.MaxValue, documentFields);
            var revisions = Database.DocumentsStorage.RevisionsStorage.GetRevisionsFrom(context, startEtag, long.MaxValue, documentFields);
            
            using var documentsEnumerator = documents.GetEnumerator(); 
            using var revisionsEnumerator = revisions.GetEnumerator(); 
            
            var enumerators = new List<IEnumerator<Document>>();
            if (documentsEnumerator.MoveNext())
                enumerators.Add(documentsEnumerator);
            if (revisionsEnumerator.MoveNext())
                enumerators.Add(revisionsEnumerator);
            while (enumerators.Count > 0)
            {
                var current = enumerators.Count > 1 && enumerators[0].Current.Etag > enumerators[1].Current.Etag
                    ? enumerators[1]
                    : enumerators[0];

                yield return current.Current;
                if (current.MoveNext() == false)
                    enumerators.Remove(current);
            }
        }
    }
}
