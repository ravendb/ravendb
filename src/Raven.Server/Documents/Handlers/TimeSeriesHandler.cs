using System;
using System.Collections.Generic;
using System.Globalization;
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
using Raven.Server.Smuggler.Documents;
using Raven.Server.TrafficWatch;
using Raven.Server.Utils;
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
            var fromList = GetStringValuesQueryString("from", required: false);
            var toList = GetStringValuesQueryString("to", required: false);

            if (fromList.Count != toList.Count)
            {
                throw new ArgumentException("Length of query string values 'from' must be equal to the length of query string values 'to'");
            }

            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
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

                            writer.WriteStartArray();

                            if (fromList.Count == 0)
                            {
                                WriteRange(context, writer, documentId, name, DateTime.MinValue, DateTime.MaxValue);
                            }
                            else
                            {
                                for (int i = 0; i < fromList.Count; i++)
                                {
                                    var (from, to) = ParseDates(fromList[i], toList[i], name);

                                    WriteRange(context, writer, documentId, name, from, to);
                                }
                            }

                            writer.WriteEndArray();

                        }
                        writer.WriteEndObject();

                    }
                    writer.WriteEndObject();

                    writer.Flush();
                }
            }
            return Task.CompletedTask;
        }

        public static (DateTime From, DateTime To) ParseDates(string fromStr, string toStr, string name)
        {
            if (DateTime.TryParseExact(fromStr, Sparrow.DefaultFormat.DateTimeOffsetFormatsToWrite,
                    CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var from) == false)
                ThrowInvalidDateTime(name, fromStr);

            if (DateTime.TryParseExact(toStr, Sparrow.DefaultFormat.DateTimeOffsetFormatsToWrite,
                    CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var to) == false)
                ThrowInvalidDateTime(name, toStr);

            return (from, to);
        }

        private void WriteRange(DocumentsOperationContext context, BlittableJsonTextWriter writer, string docId, string name, DateTime from, DateTime to)
        {
            writer.WriteStartObject();
            {
                writer.WritePropertyName(nameof(TimeSeriesRangeResult.Name));
                writer.WriteString(name);
                writer.WriteComma();

                writer.WritePropertyName(nameof(TimeSeriesRangeResult.From));
                writer.WriteDateTime(from, true);
                writer.WriteComma();

                writer.WritePropertyName(nameof(TimeSeriesRangeResult.To));
                writer.WriteDateTime(to, true);
                writer.WriteComma();

                writer.WritePropertyName(nameof(TimeSeriesRangeResult.FullRange));
                writer.WriteBool(false); // TODO: Need to figure this out
                writer.WriteComma();

                writer.WritePropertyName(nameof(TimeSeriesRangeResult.Values));
                writer.WriteStartArray();
                {
                    var reader = Database.DocumentsStorage.TimeSeriesStorage.GetReader(context, docId, name, from, to);
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
            private readonly AppendTimeSeriesOperation _singleValue;

            public string LastChangeVector;

            public ExecuteTimeSeriesBatchCommand(DocumentDatabase database, DocumentTimeSeriesOperation batch, bool fromEtl)
            {
                _database = database;
                _batch = batch;
                _fromEtl = fromEtl;

                ConvertBatch(batch, out _appendDictionary, out _singleValue);
            }

            protected override int ExecuteCmd(DocumentsOperationContext context)
            {
                int changes = 0;
                string docCollection = GetDocumentCollection(context, _batch);

                if (docCollection == null)
                    return 0;

                var tss = _database.DocumentsStorage.TimeSeriesStorage;

                if (_appendDictionary != null)
                {
                    foreach (var kvp in _appendDictionary)
                    {
                        LastChangeVector = tss.AppendTimestamp(context,
                            _batch.Id,
                            docCollection,
                            kvp.Key,
                            kvp.Value.Values
                        );

                        changes += kvp.Value.Values.Count;
                    }

                    _appendDictionary.Clear();
                }
                else if (_singleValue != null)
                {
                    LastChangeVector = tss.AppendTimestamp(context,
                        _batch.Id,
                        docCollection,
                        _singleValue.Name,
                        new []
                        {
                            _singleValue
                        });

                    changes++;
                }

                if (_batch?.Removals != null)
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
            private void ConvertBatch(DocumentTimeSeriesOperation batch, out Dictionary<string, SortedList<long, AppendTimeSeriesOperation>> appendDictionary, out AppendTimeSeriesOperation singleValue)
            {
                appendDictionary = null;
                singleValue = null;

                if (batch.Appends == null || batch.Appends.Count == 0)
                {
                    return;
                }

                if (batch.Appends.Count == 1)
                {
                    singleValue = batch.Appends[0];
                    return;
                }

                appendDictionary = new Dictionary<string, SortedList<long, AppendTimeSeriesOperation>>();

                foreach (var item in batch.Appends)
                {
                    if (appendDictionary.TryGetValue(item.Name, out var sorted) == false)
                    {
                        sorted = new SortedList<long, AppendTimeSeriesOperation>();
                        appendDictionary[item.Name] = sorted;
                    }

                    sorted[item.Timestamp.Ticks] = item;
                }
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

        internal class SmugglerTimeSeriesBatchCommand : TransactionOperationsMerger.MergedTransactionCommand
        {
            private readonly DocumentDatabase _database;

            private readonly Dictionary<string, List<TimeSeriesItem>> _dictionary;

            public string LastChangeVector;

            public SmugglerTimeSeriesBatchCommand(DocumentDatabase database)
            {
                _database = database;
                _dictionary = new Dictionary<string, List<TimeSeriesItem>>();
            }

            protected override int ExecuteCmd(DocumentsOperationContext context)
            {
                var tss = _database.DocumentsStorage.TimeSeriesStorage;

                var changes = 0;

                foreach (var (docId, items) in _dictionary)
                {
                    var collectionName = _database.DocumentsStorage.ExtractCollectionName(context, items[0].Collection);

                    foreach (var item in items)
                    {
                        using (var slicer = new TimeSeriesStorage.TimeSeriesSlicer(context, docId, item.Name, item.Baseline))
                        {
                            if (tss.TryAppendEntireSegment(context, slicer.TimeSeriesKeySlice, collectionName, item.ChangeVector, item.Segment, item.Baseline))
                            {
                                var databaseChangeVector = context.LastDatabaseChangeVector ?? DocumentsStorage.GetDatabaseChangeVector(context);
                                context.LastDatabaseChangeVector = ChangeVectorUtils.MergeVectors(databaseChangeVector, item.ChangeVector);
                                continue;
                            }
                        }

                        var changeVector = tss.AppendTimestamp(context, docId, item.Collection, item.Name, item.Segment.YieldAllValues(context, item.Baseline), item.ChangeVector);
                        context.LastDatabaseChangeVector = ChangeVectorUtils.MergeVectors(changeVector, item.ChangeVector);
                    }

                    changes += items.Count;
                }

                return changes;
            }

            public void AddToDictionary(TimeSeriesItem item)
            {
                if (_dictionary.TryGetValue(item.DocId, out var itemsList) == false)
                {
                    _dictionary[item.DocId] = itemsList = new List<TimeSeriesItem>();
                }

                itemsList.Add(item);
            }


            public override TransactionOperationsMerger.IReplayableCommandDto<TransactionOperationsMerger.MergedTransactionCommand> ToDto(JsonOperationContext context)
            {
                throw new System.NotImplementedException();
            }
        }

    }
}
