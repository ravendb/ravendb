using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using System.Threading.Tasks;
using Microsoft.Extensions.Primitives;
using Raven.Client;
using Raven.Client.Documents.Changes;
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
using Sparrow.Server;

namespace Raven.Server.Documents.Handlers
{
    public class TimeSeriesHandler : DatabaseRequestHandler
    {
        
        [RavenAction("/databases/*/timeseries/stats", "GET", AuthorizationStatus.ValidUser)]
        public Task Stats()
        {
            var documentId = GetStringQueryString("docId");
            
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                var document = Database.DocumentsStorage.Get(context, documentId, DocumentFields.Data);
                if (document == null)
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                    return Task.CompletedTask;
                }

                var timeSeriesNames = new List<string>();

                if (document.TryGetMetadata(out var metadata))
                {
                    if (metadata.TryGet(Constants.Documents.Metadata.TimeSeries, out BlittableJsonReaderArray timeSeries) && timeSeries != null)
                    {
                        foreach (object name in timeSeries)
                        {
                            if (name == null)
                                continue;

                            if (name is string || name is LazyStringValue || name is LazyCompressedStringValue)
                            {
                                timeSeriesNames.Add(name.ToString());
                            }
                        }
                    }
                }
                
                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartObject();
                    
                    writer.WritePropertyName(nameof(Client.Documents.Operations.TimeSeries.TimeSeriesStats.DocumentId));
                    writer.WriteString(documentId);
                    writer.WriteComma();
                    
                    writer.WritePropertyName(nameof(Client.Documents.Operations.TimeSeries.TimeSeriesStats.TimeSeries));
                    
                    writer.WriteStartArray();
                    
                    var first = true;
                    foreach (var tsName in timeSeriesNames)
                    {
                        if (first == false)
                        {
                            writer.WriteComma();
                        }
                        first = false;

                        var stats = Database.DocumentsStorage.TimeSeriesStorage.Stats.GetStats(context, documentId, tsName);

                        writer.WriteStartObject();
                        
                        writer.WritePropertyName(nameof(TimeSeriesItemDetail.Name));
                        writer.WriteString(tsName);
                        
                        writer.WriteComma();
                        
                        writer.WritePropertyName(nameof(TimeSeriesItemDetail.NumberOfEntries));
                        writer.WriteInteger(stats.Count);

                        writer.WriteComma();

                        writer.WritePropertyName(nameof(TimeSeriesItemDetail.StartDate));
                        writer.WriteDateTime(stats.Start, isUtc: true);

                        writer.WriteComma();

                        writer.WritePropertyName(nameof(TimeSeriesItemDetail.EndDate));
                        writer.WriteDateTime(stats.End, isUtc: true);

                        writer.WriteEndObject();
                    }
                    
                    writer.WriteEndArray();

                    writer.WriteEndObject();
                }    
            }
            
            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/timeseries", "GET", AuthorizationStatus.ValidUser)]
        public Task Read()
        {
            var documentId = GetStringQueryString("id");
            var name = GetStringQueryString("name");
            var fromList = GetStringValuesQueryString("from", required: false);
            var toList = GetStringValuesQueryString("to", required: false);

            var start = GetStart();
            var pageSize = GetPageSize();

            if (fromList.Count != toList.Count)
            {
                throw new ArgumentException("Length of query string values 'from' must be equal to the length of query string values 'to'");
            }

            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                var ranges = GetTimeSeriesRangeResults(context, documentId, name, fromList, toList, start, pageSize);

                var actualEtag = ranges.Count == 1
                    ? ranges[0].Hash
                    : CombineHashesFromMultipleRanges(ranges);

                var etag = GetStringFromHeaders("If-None-Match");
                if (etag == actualEtag)
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.NotModified;
                    return Task.CompletedTask;
                }

                HttpContext.Response.Headers[Constants.Headers.Etag] = "\"" + actualEtag + "\"";

                WriteResponse(context, documentId, name, ranges, addTotalCount: fromList.Count == 0);

                return Task.CompletedTask;
            }

        }

        private static List<TimeSeriesRangeResult> GetTimeSeriesRangeResults(DocumentsOperationContext context, string documentId, string name, StringValues fromList, StringValues toList, int start, int pageSize)
        {
            var result = new List<TimeSeriesRangeResult>();

            if (fromList.Count == 0)
            {
                var rangeResult = GetTimeSeriesRange(context, documentId, name, DateTime.MinValue, DateTime.MaxValue, ref start, ref pageSize);
                result.Add(rangeResult);
            }
            else
            {
                for (int i = 0; i < fromList.Count; i++)
                {
                    var from = ParseDate(fromList[i], name);
                    var to = ParseDate(toList[i], name);

                    var rangeResult = GetTimeSeriesRange(context, documentId, name, from, to, ref start, ref pageSize);
                    result.Add(rangeResult);
                }
            }

            return result;
        }

        internal static TimeSeriesRangeResult GetTimeSeriesRange(DocumentsOperationContext context, string docId, string name, DateTime from, DateTime to)
        {
            int start = 0, pageSize = int.MaxValue;
            return GetTimeSeriesRange(context, docId, name, from, to, ref start, ref pageSize);
        }

        internal static unsafe TimeSeriesRangeResult GetTimeSeriesRange(DocumentsOperationContext context, string docId, string name, DateTime from, DateTime to, ref int start, ref int pageSize)
        {
            var values = new List<TimeSeriesEntry>();
            var reader = context.DocumentDatabase.DocumentsStorage.TimeSeriesStorage.GetReader(context, docId, name, from, to);

            // init hash 
            var size = Sodium.crypto_generichash_bytes();
            Debug.Assert((int)size == 32);
            var cryptoGenerichashStatebytes = (int)Sodium.crypto_generichash_statebytes();
            var state = stackalloc byte[cryptoGenerichashStatebytes];
            if (Sodium.crypto_generichash_init(state, null, UIntPtr.Zero, size) != 0)
                ComputeHttpEtags.ThrowFailToInitHash();

            foreach (var (individualValues, segmentResult) in reader.SegmentsOrValues())
            {
                var enumerable = individualValues ?? segmentResult.Values;

                foreach (var singleResult in enumerable)
                {
                    if (start-- > 0)
                        continue;

                    if (pageSize-- <= 0)
                        break;

                    values.Add(new TimeSeriesEntry
                    {
                        Timestamp = singleResult.Timestamp,
                        Tag = singleResult.Tag,
                        Values = singleResult.Values.ToArray()
                    });
                }

                ComputeHttpEtags.HashChangeVector(state, segmentResult?.ChangeVector);
            }

            return new TimeSeriesRangeResult
            {
                Name = name,
                From = from,
                To = to,
                Entries = values.ToArray(),
                Hash = ComputeHttpEtags.FinalizeHash(size, state)
            };
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

        private static unsafe string CombineHashesFromMultipleRanges(List<TimeSeriesRangeResult> ranges)
        {
            // init hash 
            var size = Sodium.crypto_generichash_bytes();
            Debug.Assert((int)size == 32);
            var cryptoGenerichashStatebytes = (int)Sodium.crypto_generichash_statebytes();
            var state = stackalloc byte[cryptoGenerichashStatebytes];
            if (Sodium.crypto_generichash_init(state, null, UIntPtr.Zero, size) != 0)
                ComputeHttpEtags.ThrowFailToInitHash();

            foreach (var range in ranges)
            {
                ComputeHttpEtags.HashChangeVector(state, range.Hash);
            }

            return ComputeHttpEtags.FinalizeHash(size, state);
        }

        private void WriteRange(BlittableJsonTextWriter writer, TimeSeriesRangeResult rangeResult)
        {
            writer.WriteStartObject();
            {
                writer.WritePropertyName(nameof(TimeSeriesRangeResult.Name));
                writer.WriteString(rangeResult.Name);
                writer.WriteComma();

                writer.WritePropertyName(nameof(TimeSeriesRangeResult.From));
                writer.WriteDateTime(rangeResult.From, true);
                writer.WriteComma();

                writer.WritePropertyName(nameof(TimeSeriesRangeResult.To));
                writer.WriteDateTime(rangeResult.To, true);
                writer.WriteComma();

                writer.WritePropertyName(nameof(TimeSeriesRangeResult.FullRange));
                writer.WriteBool(false); // TODO: Need to figure this out
                writer.WriteComma();

                writer.WritePropertyName(nameof(TimeSeriesRangeResult.Entries));
                writer.WriteStartArray();
                {
                    for (var i = 0; i < rangeResult.Entries.Length; i++)
                    {
                        if (i > 0)
                            writer.WriteComma();

                        writer.WriteStartObject();

                        writer.WritePropertyName(nameof(TimeSeriesEntry.Timestamp));
                        writer.WriteDateTime(rangeResult.Entries[i].Timestamp, true);
                        writer.WriteComma();
                        writer.WritePropertyName(nameof(TimeSeriesEntry.Tag));
                        writer.WriteString(rangeResult.Entries[i].Tag);
                        writer.WriteComma();
                        writer.WriteArray(nameof(TimeSeriesEntry.Values), rangeResult.Entries[i].Values);

                        writer.WriteEndObject();
                    }
                }
                writer.WriteEndArray();
            }
            writer.WriteEndObject();
        }

        private void WriteResponse(DocumentsOperationContext context, string documentId, string name, List<TimeSeriesRangeResult> ranges, bool addTotalCount)
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

                        foreach (var range in ranges)
                        {
                            WriteRange(writer, range);
                        }

                        writer.WriteEndArray();
                    }
                    writer.WriteEndObject();

                    writer.WriteComma();
                    writer.WritePropertyName(nameof(TimeSeriesDetails.TotalResults));

                    if (addTotalCount)
                    {
                        // add total entries count to the response 
                        var stats = context.DocumentDatabase.DocumentsStorage.TimeSeriesStorage.Stats.GetStats(context, documentId, name);
                        writer.WriteInteger(stats.Count);
                    }
                    else
                    {
                        writer.WriteInteger(-1);
                    }
                }
                writer.WriteEndObject();

                writer.Flush();
            }
        }

        [RavenAction("/databases/*/timeseries", "POST", AuthorizationStatus.ValidUser)]
        public async Task Batch()
        {
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                var blittable = await context.ReadForMemoryAsync(RequestBodyStream(), "timeseries");

                var operation = JsonDeserializationClient.TimeSeriesOperation(blittable);

                if (TrafficWatchManager.HasRegisteredClients)
                    AddStringToHttpContext(blittable.ToString(), TrafficWatchChangeType.TimeSeries);

                var cmd = new ExecuteTimeSeriesBatchCommand(Database, operation, false);

                try
                {
                    await Database.TxMerger.Enqueue(cmd);
                    NoContentStatus();
                }
                catch (DocumentDoesNotExistException)
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                    throw;
                }
            }
        }

        [RavenAction("/databases/*/admin/timeseries/config", "POST", AuthorizationStatus.DatabaseAdmin)]
        public async Task ConfigTimeSeries()
        {
            await DatabaseConfigurations(
                ServerStore.ModifyTimeSeriesConfiguration,
                "read-timeseries-config", 
                GetRaftRequestIdFromQuery(),
                beforeSetupConfiguration: (string name, ref BlittableJsonReaderObject configuration, JsonOperationContext context) =>
                {
                    if (configuration == null || 
                        configuration.TryGet(nameof(TimeSeriesConfiguration.Collections), out BlittableJsonReaderObject collections) == false ||
                        collections?.Count > 0 == false)
                        return;

                    var uniqueKeys = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                    var prop = new BlittableJsonReaderObject.PropertyDetails();

                    for (var i = 0; i < collections.Count; i++)
                    {
                        collections.GetPropertyByIndex(i, ref prop);

                        if (uniqueKeys.Add(prop.Name) == false)
                        {
                            throw new InvalidOperationException("Cannot have two different revision configurations on the same collection. " +
                                                                $"Collection name : '{prop.Name}'");
                        }
                    }
                });
        }
        public class ExecuteTimeSeriesBatchCommand : TransactionOperationsMerger.MergedTransactionCommand
        {
            private readonly DocumentDatabase _database;
            private readonly TimeSeriesOperation _operation;
            private readonly bool _fromEtl;

            public string LastChangeVector;

            public ExecuteTimeSeriesBatchCommand(DocumentDatabase database, TimeSeriesOperation operation, bool fromEtl)
            {
                _database = database;
                _operation = operation;
                _fromEtl = fromEtl;
            }

            protected override long ExecuteCmd(DocumentsOperationContext context)
            {
                string docCollection = GetDocumentCollection(_database, context, _operation.DocumentId, _fromEtl);

                if (docCollection == null)
                    return 0L;

                var changes = 0L;
                var tss = _database.DocumentsStorage.TimeSeriesStorage;

                if (_operation.Removals?.Count > 0)
                {
                    foreach (var removal in _operation.Removals)
                    {
                        var deletionRange = new TimeSeriesStorage.DeletionRangeRequest
                        {
                            DocumentId = _operation.DocumentId,
                            Collection = docCollection,
                            Name = _operation.Name,
                            From = removal.From,
                            To = removal.To
                        };

                        LastChangeVector = tss.RemoveTimestampRange(context, deletionRange);

                        changes++;
                    }
                }

                if (_operation.Appends?.Count > 0 == false)
                    return changes;

                if (_operation.Appends.Count == 1)
                {
                    LastChangeVector = tss.AppendTimestamp(context,
                        _operation.DocumentId,
                        docCollection,
                        _operation.Name,
                        new[] {_operation.Appends[0]});

                    changes++;
                }
                else
                {
                    LastChangeVector = tss.AppendTimestamp(context,
                        _operation.DocumentId,
                        docCollection,
                        _operation.Name,
                        _operation.Appends
                    );

                    changes += _operation.Appends.Count;
                }

                return changes;
            }

            public static string GetDocumentCollection(DocumentDatabase database, DocumentsOperationContext context, string documentId, bool fromEtl)
            {
                try
                {
                    var doc = database.DocumentsStorage.Get(context, documentId, throwOnConflict: true);
                    if (doc == null)
                    {
                        if (fromEtl)
                            return null;

                        ThrowMissingDocument(documentId);
                        return null;// never hit
                    }

                    if (doc.Flags.HasFlag(DocumentFlags.Artificial))
                        ThrowArtificialDocument(doc);

                    return CollectionName.GetCollectionName(doc.Data);
                }
                catch (DocumentConflictException)
                {
                    if (fromEtl)
                        return null;

                    // this is fine, we explicitly support
                    // setting the flag if we are in conflicted state is 
                    // done by the conflict resolver

                    // avoid loading same document again, we validate write using the metadata instance
                    return database.DocumentsStorage.ConflictsStorage.GetCollection(context, documentId);
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

            protected override long ExecuteCmd(DocumentsOperationContext context)
            {
                var tss = _database.DocumentsStorage.TimeSeriesStorage;

                var changes = 0L;

                foreach (var (docId, items) in _dictionary)
                {
                    var collectionName = _database.DocumentsStorage.ExtractCollectionName(context, items[0].Collection);

                    foreach (var item in items)
                    {
                        using (var slicer = new TimeSeriesSliceHolder(context, docId, item.Name).WithBaseline(item.Baseline))
                        {
                            // on import we remove all @time-series from the document, so we need to re-add them
                            var newSeries = tss.Stats.GetStats(context, slicer) == default;

                            if (tss.TryAppendEntireSegment(context, slicer.TimeSeriesKeySlice, collectionName, item))
                            {
                                var databaseChangeVector = context.LastDatabaseChangeVector ?? DocumentsStorage.GetDatabaseChangeVector(context);
                                context.LastDatabaseChangeVector = ChangeVectorUtils.MergeVectors(databaseChangeVector, item.ChangeVector);
                                
                                if (newSeries)
                                    tss.AddTimeSeriesNameToMetadata(context, item.DocId, item.Name);
                                
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
