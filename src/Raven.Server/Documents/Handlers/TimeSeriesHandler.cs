using System;
using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Documents.Session.TimeSeries;
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
                                    var from = ParseDate(fromList[i], name);
                                    var to = ParseDate(toList[i], name);

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

        public static unsafe DateTime ParseDate(string dateStr, string name)
        {
            fixed (char* c = dateStr)
            {
                var result = LazyStringParser.TryParseDateTime(c, dateStr.Length, out var dt, out _);
                if (result != LazyStringParser.Result.DateTime)
                    ThrowInvalidDateTime(name, dateStr);

                return dt;
            }
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

                writer.WritePropertyName(nameof(TimeSeriesRangeResult.Entries));
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

                        writer.WritePropertyName(nameof(TimeSeriesEntry.Timestamp));
                        writer.WriteDateTime(item.Timestamp, true);
                        writer.WriteComma();
                        writer.WritePropertyName(nameof(TimeSeriesEntry.Tag));
                        writer.WriteString(item.Tag);
                        writer.WriteComma();
                        writer.WriteArray(nameof(TimeSeriesEntry.Values), item.Values);

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

                var timeSeriesBatch = JsonDeserializationClient.TimeSeriesBatch(blittable);

                if (TrafficWatchManager.HasRegisteredClients)
                    AddStringToHttpContext(blittable.ToString(), TrafficWatchChangeType.TimeSeries);

                var cmd = new ExecuteTimeSeriesBatchCommand(Database, timeSeriesBatch.Documents, false);

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
            private readonly List<TimeSeriesOperation> _operations;
            private readonly bool _fromEtl;

            private Dictionary<string, SortedList<long, TimeSeriesOperation.AppendOperation>> _appendDictionary;
            private TimeSeriesOperation.AppendOperation _singleValue;

            public string LastChangeVector;

            public ExecuteTimeSeriesBatchCommand(DocumentDatabase database, List<TimeSeriesOperation> operations, bool fromEtl)
            {
                _database = database;
                _operations = operations;
                _fromEtl = fromEtl;
            }

            protected override int ExecuteCmd(DocumentsOperationContext context)
            {
                int changes = 0;

                foreach (var operation in _operations)
                {
                    string docCollection = GetDocumentCollection(context, operation);

                    if (docCollection == null)
                        continue;

                    ConvertBatch(operation);

                    var tss = _database.DocumentsStorage.TimeSeriesStorage;

                    if (_appendDictionary != null)
                    {
                        foreach (var kvp in _appendDictionary)
                        {
                            LastChangeVector = tss.AppendTimestamp(context,
                                operation.DocumentId,
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
                            operation.DocumentId,
                            docCollection,
                            _singleValue.Name,
                            new[]
                            {
                            _singleValue
                            });

                        changes++;
                    }

                    if (operation?.Removals != null)
                    {
                        foreach (var removal in operation.Removals)
                        {
                            LastChangeVector = tss.RemoveTimestampRange(context,
                                operation.DocumentId,
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

            private void ConvertBatch(TimeSeriesOperation operation)
            {
                _appendDictionary?.Clear();
                _singleValue = null;

                if (operation.Appends == null || operation.Appends.Count == 0)
                {
                    return;
                }

                if (operation.Appends.Count == 1)
                {
                    _singleValue = operation.Appends[0];
                    return;
                }

                _appendDictionary = new Dictionary<string, SortedList<long, TimeSeriesOperation.AppendOperation>>();

                foreach (var item in operation.Appends)
                {
                    if (_appendDictionary.TryGetValue(item.Name, out var sorted) == false)
                    {
                        sorted = new SortedList<long, TimeSeriesOperation.AppendOperation>();
                        _appendDictionary[item.Name] = sorted;
                    }

                    sorted[item.Timestamp.Ticks] = item;
                }
            }

            private string GetDocumentCollection(DocumentsOperationContext context, TimeSeriesOperation operation)
            {
                try
                {
                    var doc = _database.DocumentsStorage.Get(context, operation.DocumentId,
                         throwOnConflict: true);
                    if (doc == null)
                    {
                        if (_fromEtl)
                            return null;

                        ThrowMissingDocument(operation.DocumentId);
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
                    return _database.DocumentsStorage.ConflictsStorage.GetCollection(context, operation.DocumentId);
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

                        var values = item.Segment.YieldAllValues(context, context.Allocator, item.Baseline);
                        var changeVector = tss.AppendTimestamp(context, docId, item.Collection, item.Name, values, item.ChangeVector);
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
