using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions.Documents;
using Raven.Client.Json.Converters;
using Raven.Server.Documents.TimeSeries;
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
            using (context.OpenReadTransaction())
            {
                var reader = Database.DocumentsStorage.TimeSeriesStorage.GetReader(context, documentId, name, from, to);

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartObject();
                    {

                        writer.WritePropertyName(nameof(TimeSeriesDetails.Id));
                        writer.WriteString(documentId);
                        writer.WriteComma();

                        writer.WritePropertyName(nameof(TimeSeriesDetails.Values));
                        writer.WriteStartObject();
                        {

                            writer.WritePropertyName(name);

                            writer.WriteStartObject();
                            {

                                writer.WritePropertyName(nameof(TimeSeriesRange.Name));
                                writer.WriteString(name);
                                writer.WriteComma();

                                writer.WritePropertyName(nameof(TimeSeriesRange.From));
                                writer.WriteDateTime(from, true);
                                writer.WriteComma();


                                writer.WritePropertyName(nameof(TimeSeriesRange.To));
                                writer.WriteDateTime(to, true);
                                writer.WriteComma();

                                writer.WritePropertyName(nameof(TimeSeriesRange.FullRange));
                                writer.WriteBool(false); // TODO: Need to figure this out
                                writer.WriteComma();

                                writer.WritePropertyName(nameof(TimeSeriesRange.Values));
                                writer.WriteStartArray();
                                {
                                    var first = true;
                                    foreach (var item in reader.AllValues())
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

                                        writer.WritePropertyName(nameof(TimeSeriesValue.Timestamp));
                                        writer.WriteDateTime(item.TimeStamp, true);
                                        writer.WriteComma();
                                        writer.WritePropertyName(nameof(TimeSeriesValue.Tag));
                                        writer.WriteString(item.Tag);
                                        writer.WriteComma();
                                        writer.WriteArray(nameof(TimeSeriesValue.Values), item.Values);

                                        writer.WriteEndObject();
                                    }
                                }
                                writer.WriteEndArray();


                            }
                            writer.WriteEndObject();

                        }
                        writer.WriteEndObject();

                    }
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

                var timeSeriesBatch = JsonDeserializationClient.DocumentTimeSeriesOperation(blittable);

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
            private readonly DocumentTimeSeriesOperation _batch;
            private readonly bool _fromEtl;

            private readonly Dictionary<string, SortedList<long, AppendTimeSeriesOperation>> _appendDictionary;

            public string LastChangeVector;

            public ExecuteTimeSeriesBatchCommand(DocumentDatabase database, DocumentTimeSeriesOperation batch, bool fromEtl)
            {
                _database = database;
                _batch = batch;
                _fromEtl = fromEtl;

                if (batch.Appends?.Count > 1)
                {
                    _appendDictionary = new Dictionary<string, SortedList<long, AppendTimeSeriesOperation>>();
             
                    foreach (var item in batch.Appends)
                    {
                        if (_appendDictionary.TryGetValue(item.Name, out var sorted) == false)
                            sorted = new SortedList<long, AppendTimeSeriesOperation>();

                        sorted.Add(item.Timestamp.Ticks, item);
                        _appendDictionary[item.Name] = sorted;
                    }
                }
            }

            protected override int ExecuteCmd(DocumentsOperationContext context)
            {
                int changes = 0;
                string docCollection = GetDocumentCollection(context, _batch);

                if (docCollection == null)
                    return 0;

                var tss = _database.DocumentsStorage.TimeSeriesStorage;

                var holder = new TimeSeriesStorage.Reader.SingleResult();
                if (_appendDictionary != null)
                {
                    foreach (var kvp in _appendDictionary)
                    {
                        LastChangeVector = tss.AppendTimestamp(context,
                            _batch.Id,
                            docCollection,
                            kvp.Key,
                            kvp.Value.Values.Select(x =>
                            {
                                holder.Values = x.Values;
                                holder.Tag = context.GetLazyString(x.Tag);
                                holder.TimeStamp = x.Timestamp;
                                return holder;
                            })
                        );

                        changes += kvp.Value.Count;
                    }
                }
                else if (_batch.Appends != null)
                {
                    LastChangeVector = tss.AppendTimestamp(context,
                        _batch.Id,
                        docCollection,
                        _batch.Appends[0].Name,
                        _batch.Appends.Select(x =>
                        {
                            holder.Values = x.Values;
                            holder.Tag = context.GetLazyString(x.Tag);
                            holder.TimeStamp = x.Timestamp;
                            return holder;
                        })
                    );

                    changes++;
                }
                

                if (_batch.Removals != null)
                {
                    foreach (var removal in _batch.Removals)
                    {
                        LastChangeVector = tss.RemoveTimestampRange(context,
                            _batch.Id,
                            docCollection,
                            removal.Name,
                            removal.From,
                            removal.To
                        );
                        changes++;
                    }
                }
                return changes;
            }

            private string GetDocumentCollection(DocumentsOperationContext context, DocumentTimeSeriesOperation docBatch)
            {
                try
                {
                    var doc = _database.DocumentsStorage.Get(context, docBatch.Id,
                         throwOnConflict: true);
                    if (doc == null)
                    {
                        if (_fromEtl)
                            return null;

                        ThrowMissingDocument(docBatch.Id);
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
                    return _database.DocumentsStorage.ConflictsStorage.GetCollection(context, docBatch.Id);
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
