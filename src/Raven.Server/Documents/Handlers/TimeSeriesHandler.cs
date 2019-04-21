using System;
using System.Net;
using System.Threading.Tasks;
using Microsoft.CodeAnalysis;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Exceptions.Documents;
using Raven.Client.Json.Converters;
using Raven.Server.Commercial;
using Raven.Server.Documents.Patch;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.TrafficWatch;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers
{
    public class TimeSeriesHandler : DatabaseRequestHandler
    {

        [RavenAction("/databases/*/timeseries", "GET", AuthorizationStatus.ValidUser)]
        public Task Read()
        {
            var documentId = GetStringQueryString("id");
            var name = GetStringQueryString("name");
            var from = GetDateTimeQueryString("from", required: false) ?? DateTime.MinValue;
            var to = GetDateTimeQueryString("to", required: false) ?? DateTime.MaxValue;


            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using(context.OpenReadTransaction())
            {
                var reader = Database.DocumentsStorage.TimeSeriesStorage.GetReader(context, documentId, name, from, to);

                if(reader.Init() == false)
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                    return Task.CompletedTask;
                }

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartObject();

                    writer.WritePropertyName("DocumentId");
                    writer.WriteString(documentId);
                    writer.WriteComma();

                    writer.WritePropertyName("Name");
                    writer.WriteString(name);
                    writer.WriteComma();

                    writer.WritePropertyName("Values");
                    writer.WriteStartArray();

                    var first = true;
                    foreach (var item in reader.Values())
                    {
                        if (first)
                        {
                            first = false;
                        }
                        else
                        {
                            writer.WriteComma();
                        }
                        writer.WriteStartObject();

                        writer.WritePropertyName("TimeStamp");
                        writer.WriteDateTime(item.TimeStamp, true);
                        writer.WriteComma();
                        writer.WritePropertyName("Tag");
                        writer.WriteString(item.Tag.ToString());//TODO: Avoid this string allocation!
                        writer.WriteComma();
                        writer.WriteArray("Values", item.Values);

                        writer.WriteEndObject();
                    }

                    writer.WriteEndArray();



                    writer.WriteEndObject();
                    
                    writer.Flush();
                }
            }
            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/timeseries", "POST", AuthorizationStatus.ValidUser)]
        public async Task Batch()
        {
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                var blittable = await context.ReadForMemoryAsync(RequestBodyStream(), "timeseries");

                var timeSeriesBatch = JsonDeserializationClient.TimeSeriesBatch(blittable);

                if (TrafficWatchManager.HasRegisteredClients)
                    AddStringToHttpContext(blittable.ToString(), TrafficWatchChangeType.TimeSeries);
                
                var cmd = new ExecuteTimeSeriesBatchCommand(Database, timeSeriesBatch, false);

                try
                {
                    await Database.TxMerger.Enqueue(cmd);
                }
                catch (DocumentDoesNotExistException)
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                    throw;
                }
            }
        }

        
        public class ExecuteTimeSeriesBatchCommand : TransactionOperationsMerger.MergedTransactionCommand
        {
            private readonly DocumentDatabase _database;
            private readonly TimeSeriesBatch _batch;
            private readonly bool _fromEtl;

            public ExecuteTimeSeriesBatchCommand(DocumentDatabase database, TimeSeriesBatch batch, bool fromEtl)
            {
                _database = database;
                _batch = batch;
                _fromEtl = fromEtl;
            }

            protected override int ExecuteCmd(DocumentsOperationContext context)
            {
                int changes = 0;
                foreach (var docBatch in _batch.Documents)
                {
                    string docCollection = GetDocumentCollection(context, docBatch);

                    if(docCollection == null)
                        continue;
                    
                    if (docBatch.Appends != null)
                    {
                        foreach (var append in docBatch.Appends)
                        {
                            _database.DocumentsStorage.TimeSeriesStorage.AppendTimestamp(context,
                                docBatch.DocumentId,
                                docCollection,
                                append.Name,
                                append.Timestamp,
                                new Span<double>(append.Values),
                                append.Tag,
                                fromReplication: false
                            );
                            changes++;
                        }
                    }
                    
                    if (docBatch.Removals != null)
                    {
                        foreach (var removal in docBatch.Removals)
                        {
                            _database.DocumentsStorage.TimeSeriesStorage.RemoveTimestampRange(context,
                                docBatch.DocumentId,
                                docCollection,
                                removal.Name,
                                removal.From,
                                removal.To
                            );
                            changes++;
                        }
                    }
                }

                return changes;
            }

            private string GetDocumentCollection(DocumentsOperationContext context, DocumentTimeSeriesOperation docBatch)
            {
                try
                {
                   var doc = _database.DocumentsStorage.Get(context, docBatch.DocumentId,
                        throwOnConflict: true);
                    if (doc == null)
                    {
                        if (_fromEtl)
                            return null;

                        ThrowMissingDocument(docBatch.DocumentId);
                        return null;// never hit
                    }

                    if (doc.Flags.HasFlag(DocumentFlags.Artificial))
                        ThrowArtificialDocument(doc);

                    return CollectionName.GetCollectionName(doc.Data);
                }
                catch (DocumentConflictException)
                {
                    if (_fromEtl)
                        return null;

                    // this is fine, we explicitly support
                    // setting the flag if we are in conflicted state is 
                    // done by the conflict resolver

                    // avoid loading same document again, we validate write using the metadata instance
                    return _database.DocumentsStorage.ConflictsStorage.GetCollection(context, docBatch.DocumentId);
                }
            }

            private static void ThrowMissingDocument(string docId)
            {
                throw new DocumentDoesNotExistException(docId, "Cannot operate on time series of a missing document");
            }
            
            
            public static void ThrowArtificialDocument(Document doc)
            {
                throw new InvalidOperationException($"Document '{doc.Id}' has '{nameof(DocumentFlags.Artificial)}' flag set. " +
                                                    "Cannot put TimeSeries on artificial documents.");
            }


            public override TransactionOperationsMerger.IReplayableCommandDto<TransactionOperationsMerger.MergedTransactionCommand> ToDto(JsonOperationContext context)
            {
                throw new System.NotImplementedException();
            }
        }
        
    }
}
