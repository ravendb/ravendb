using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Primitives;
using Raven.Client;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Documents.Session.TimeSeries;
using Raven.Client.Exceptions.Documents;
using Raven.Server.Documents.Handlers.Processors.Configuration;
using Raven.Server.Documents.Handlers.Processors.TimeSeries;
using Raven.Server.Documents.Includes;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Server.Smuggler.Documents;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Platform;

namespace Raven.Server.Documents.Handlers
{
    public class TimeSeriesHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/timeseries/stats", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task Stats()
        {
            using (var processor = new TimeSeriesHandlerProcessorForGetTimeSeriesStats(this))
            {
                await processor.ExecuteAsync();
            }
        }

        internal static List<string> GetTimesSeriesNames(Document document)
        {
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

            return timeSeriesNames;
        }

        [RavenAction("/databases/*/timeseries/ranges", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task ReadRanges()
        {
            var documentId = GetStringQueryString("docId");
            var names = GetStringValuesQueryString("name");
            var fromList = GetStringValuesQueryString("from");
            var toList = GetStringValuesQueryString("to");

            var start = GetStart();
            var pageSize = GetPageSize();

            var includeDoc = GetBoolValueQueryString("includeDocument", required: false) ?? false;
            var includeTags = GetBoolValueQueryString("includeTags", required: false) ?? false;
            var returnFullResults = GetBoolValueQueryString("full", required: false) ?? false;

            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                var includesCommand = includeDoc || includeTags
                    ? new IncludeDocumentsDuringTimeSeriesLoadingCommand(context, documentId, includeDoc, includeTags)
                    : null;

                var ranges = GetTimeSeriesRangeResults(context, documentId, names, fromList, toList, start, pageSize, includesCommand, returnFullResults);

                var actualEtag = CombineHashesFromMultipleRanges(ranges);

                var etag = GetStringFromHeaders("If-None-Match");
                if (etag == actualEtag)
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.NotModified;
                    return;
                }

                HttpContext.Response.Headers[Constants.Headers.Etag] = "\"" + actualEtag + "\"";

                await WriteResponse(context, documentId, ranges, Database.DatabaseShutdown);
            }
        }

        [RavenAction("/databases/*/timeseries", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task Read()
        {
            using (var processor = new TimeSeriesHandlerProcessorForGetTimeSeries(this))
                await processor.ExecuteAsync();
        }

        private static Dictionary<string, List<TimeSeriesRangeResult>> GetTimeSeriesRangeResults(DocumentsOperationContext context, string documentId, StringValues names, StringValues fromList, StringValues toList, int start, int pageSize,
            IncludeDocumentsDuringTimeSeriesLoadingCommand includes, bool returnFullResult = false)
        {
            if (fromList.Count == 0)
                throw new ArgumentException("Length of query string values 'from' must be greater than zero");

            if (fromList.Count != toList.Count)
                throw new ArgumentException("Length of query string values 'from' must be equal to the length of query string values 'to'");

            if (fromList.Count != names.Count)
                throw new InvalidOperationException($"GetMultipleTimeSeriesOperation : Argument count miss match on document '{documentId}'. " +
                                                    $"Received {names.Count} 'name' arguments, and {fromList.Count} 'from'/'to' arguments.");

            var rangeResultDictionary = new Dictionary<string, List<TimeSeriesRangeResult>>(StringComparer.OrdinalIgnoreCase);

            for (int i = 0; i < fromList.Count; i++)
            {
                var name = names[i];

                if (string.IsNullOrEmpty(name))
                    throw new InvalidOperationException($"GetMultipleTimeSeriesOperation : Missing '{nameof(TimeSeriesRange.Name)}' argument in 'TimeSeriesRange' on document '{documentId}'. " +
                                                        $"'{nameof(TimeSeriesRange.Name)}' cannot be null or empty");

                var from = string.IsNullOrEmpty(fromList[i]) ? DateTime.MinValue : TimeSeriesHandlerProcessorForGetTimeSeries.ParseDate(fromList[i], name);
                var to = string.IsNullOrEmpty(toList[i]) ? DateTime.MaxValue : TimeSeriesHandlerProcessorForGetTimeSeries.ParseDate(toList[i], name);

                bool incrementalTimeSeries = TimeSeriesHandlerProcessorForGetTimeSeries.CheckIfIncrementalTs(name);

                var rangeResult = incrementalTimeSeries ?
                    TimeSeriesHandlerProcessorForGetTimeSeries.GetIncrementalTimeSeriesRange(context, documentId, name, from, to, ref start, ref pageSize, includes, returnFullResult) :
                    TimeSeriesHandlerProcessorForGetTimeSeries.GetTimeSeriesRange(context, documentId, name, from, to, ref start, ref pageSize, includes);

                if (rangeResult == null)
                {
                    Debug.Assert(pageSize <= 0, "Page size must be zero or less here");
                    return rangeResultDictionary;
                }
                if (rangeResultDictionary.TryGetValue(name, out var list) == false)
                {
                    rangeResultDictionary[name] = new List<TimeSeriesRangeResult> { rangeResult };
                }
                else
                {
                    list.Add(rangeResult);
                }

                if (pageSize <= 0)
                    break;
            }

            return rangeResultDictionary;
        }

        private static void MergeIncrementalTimeSeriesValues(SingleResult singleResult, string nodeTag, double[] values, ref TimeSeriesEntry entry, bool returnFullResults)
        {
            if (entry.Values.Length < values.Length) // need to allocate more space for new values
            {
                var updatedValues = singleResult.Values.Span;

                for (int i = 0; i < entry.Values.Length; i++)
                    updatedValues[i] += entry.Values[i];

                entry.Values = updatedValues.ToArray();
            }
            else
            {
                for (int i = 0; i < values.Length; i++)
                    entry.Values[i] += values[i];
            }

            if (returnFullResults == false)
                return;

            if (entry.NodeValues.TryGetValue(nodeTag, out var nodeValues))
            {
                if (nodeValues.Length < values.Length) // need to allocate more space for new values
                {
                    for (int i = 0; i < nodeValues.Length; i++)
                        values[i] += nodeValues[i];

                    entry.NodeValues[nodeTag] = values;
                    return;
                }

                for (int i = 0; i < values.Length; i++)
                    nodeValues[i] += values[i];
            }
            else
                entry.NodeValues[nodeTag] = values;
        }

        private static unsafe string CombineHashesFromMultipleRanges(Dictionary<string, List<TimeSeriesRangeResult>> ranges)
        {
            // init hash
            var size = Sodium.crypto_generichash_bytes();
            Debug.Assert((int)size == 32);
            var cryptoGenerichashStatebytes = (int)Sodium.crypto_generichash_statebytes();
            var state = stackalloc byte[cryptoGenerichashStatebytes];
            if (Sodium.crypto_generichash_init(state, null, UIntPtr.Zero, size) != 0)
                ComputeHttpEtags.ThrowFailToInitHash();

            ComputeHttpEtags.HashNumber(state, ranges.Count);

            foreach (var kvp in ranges)
            {
                foreach (var range in kvp.Value)
                {
                    ComputeHttpEtags.HashChangeVector(state, range.Hash);
                }
            }

            return ComputeHttpEtags.FinalizeHash(size, state);
        }

        private async Task WriteResponse(DocumentsOperationContext context, string documentId, Dictionary<string, List<TimeSeriesRangeResult>> ranges, CancellationToken token)
        {
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteStartObject();
                {
                    writer.WritePropertyName(nameof(TimeSeriesDetails.Id));
                    writer.WriteString(documentId);

                    writer.WriteComma();
                    writer.WritePropertyName(nameof(TimeSeriesDetails.Values));
                    await WriteTimeSeriesRangeResultsAsync(context, writer, documentId, ranges, token);
                }
                writer.WriteEndObject();
            }
        }

        internal static async Task WriteTimeSeriesRangeResultsAsync(DocumentsOperationContext context, AsyncBlittableJsonTextWriter writer, string documentId, Dictionary<string, List<TimeSeriesRangeResult>> dictionary, CancellationToken token)
        {
            if (dictionary == null)
            {
                writer.WriteNull();
                return;
            }

            writer.WriteStartObject();

            bool first = true;
            foreach (var (name, ranges) in dictionary)
            {
                if (first == false)
                    writer.WriteComma();

                first = false;

                writer.WritePropertyName(name);

                writer.WriteStartArray();

                (long Count, DateTime Start, DateTime End) stats = default;
                if (documentId != null)
                {
                    Debug.Assert(context != null);
                    stats = context.DocumentDatabase.DocumentsStorage.TimeSeriesStorage.Stats.GetStats(context, documentId, name);
                }

                for (var i = 0; i < ranges.Count; i++)
                {
                    long? totalCount = null;

                    if (i > 0)
                        writer.WriteComma();

                    if (stats != default && ranges[i].From <= stats.Start && ranges[i].To >= stats.End)
                    {
                        totalCount = stats.Count;
                    }

                    TimeSeriesHandlerProcessorForGetTimeSeries.WriteRange(writer, ranges[i], totalCount);

                    await writer.MaybeFlushAsync(token);
                }
                writer.WriteEndArray();
            }

            writer.WriteEndObject();
        }

        internal static int WriteTimeSeriesRangeResults(DocumentsOperationContext context, AsyncBlittableJsonTextWriter writer, string documentId, Dictionary<string, List<TimeSeriesRangeResult>> dictionary)
        {
            if (dictionary == null)
            {
                writer.WriteNull();
                return 0;
            }

            writer.WriteStartObject();

            int size = 0;
            bool first = true;
            foreach (var (name, ranges) in dictionary)
            {
                if (first == false)
                    writer.WriteComma();

                first = false;

                writer.WritePropertyName(name);
                size += name.Length;

                writer.WriteStartArray();

                (long Count, DateTime Start, DateTime End) stats = default;
                if (documentId != null)
                {
                    Debug.Assert(context != null);
                    stats = context.DocumentDatabase.DocumentsStorage.TimeSeriesStorage.Stats.GetStats(context, documentId, name);
                }

                for (var i = 0; i < ranges.Count; i++)
                {
                    long? totalCount = null;

                    if (i > 0)
                        writer.WriteComma();

                    if (stats != default && ranges[i].From <= stats.Start && ranges[i].To >= stats.End)
                    {
                        totalCount = stats.Count;
                    }

                    size += TimeSeriesHandlerProcessorForGetTimeSeries.WriteRange(writer, ranges[i], totalCount);
                }

                writer.WriteEndArray();
            }

            writer.WriteEndObject();

            return size;
        }

        [RavenAction("/databases/*/timeseries", "POST", AuthorizationStatus.ValidUser, EndpointType.Write)]
        public async Task Batch()
        {
            using (var processor = new TimeSeriesHandlerProcessorForPostTimeSeries(this))
            {
                await processor.ExecuteAsync();
            }
        }

        [RavenAction("/databases/*/timeseries/config", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task GetTimeSeriesConfiguration()
        {
            using (var processor = new ConfigurationHandlerProcessorForGetTimeSeriesConfiguration(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/admin/timeseries/config", "POST", AuthorizationStatus.DatabaseAdmin)]
        public async Task PostTimeSeriesConfiguration()
        {
            using (var processor = new ConfigurationHandlerProcessorForPostTimeSeriesConfiguration(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/admin/timeseries/policy", "PUT", AuthorizationStatus.DatabaseAdmin)]
        public async Task AddTimeSeriesPolicy()
        {
            await ServerStore.EnsureNotPassiveAsync();
            var collection = GetStringQueryString("collection", required: true);

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (var json = await context.ReadForDiskAsync(RequestBodyStream(), "time-series policy config"))
            {
                var policy = JsonDeserializationCluster.TimeSeriesPolicy(json);

                TimeSeriesConfiguration current;
                using (context.OpenReadTransaction())
                {
                    current = ServerStore.Cluster.ReadRawDatabaseRecord(context, Database.Name).TimeSeriesConfiguration ?? new TimeSeriesConfiguration();
                }

                current.Collections ??= new Dictionary<string, TimeSeriesCollectionConfiguration>(StringComparer.OrdinalIgnoreCase);

                if (current.Collections.ContainsKey(collection) == false)
                    current.Collections[collection] = new TimeSeriesCollectionConfiguration();

                if (RawTimeSeriesPolicy.IsRaw(policy))
                    current.Collections[collection].RawPolicy = new RawTimeSeriesPolicy(policy.RetentionTime);
                else
                {
                    current.Collections[collection].Policies ??= new List<TimeSeriesPolicy>();
                    var existing = current.Collections[collection].GetPolicyByName(policy.Name, out _);
                    if (existing != null)
                        current.Collections[collection].Policies.Remove(existing);

                    current.Collections[collection].Policies.Add(policy);
                }

                current.InitializeRollupAndRetention();

                ServerStore.LicenseManager.AssertCanAddTimeSeriesRollupsAndRetention(current);

                var editTimeSeries = new EditTimeSeriesConfigurationCommand(current, Database.Name, GetRaftRequestIdFromQuery());
                var (index, _) = await ServerStore.SendToLeaderAsync(editTimeSeries);

                await WaitForIndexToBeAppliedAsync(context, index);
                await SendConfigurationResponseAsync(context, index);
            }
        }

        [RavenAction("/databases/*/admin/timeseries/policy", "DELETE", AuthorizationStatus.DatabaseAdmin)]
        public async Task RemoveTimeSeriesPolicy()
        {
            await ServerStore.EnsureNotPassiveAsync();
            var collection = GetStringQueryString("collection", required: true);
            var name = GetStringQueryString("name", required: true);

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                TimeSeriesConfiguration current;
                using (context.OpenReadTransaction())
                {
                    current = ServerStore.Cluster.ReadRawDatabaseRecord(context, Database.Name).TimeSeriesConfiguration;
                }

                if (current?.Collections?.ContainsKey(collection) == true)
                {
                    var p = current.Collections[collection].GetPolicyByName(name, out _);
                    if (p == null)
                        return;

                    if (ReferenceEquals(p, current.Collections[collection].RawPolicy))
                    {
                        current.Collections[collection].RawPolicy = RawTimeSeriesPolicy.Default;
                    }
                    else
                    {
                        current.Collections[collection].Policies.Remove(p);
                    }

                    current.InitializeRollupAndRetention();

                    ServerStore.LicenseManager.AssertCanAddTimeSeriesRollupsAndRetention(current);

                    var editTimeSeries = new EditTimeSeriesConfigurationCommand(current, Database.Name, GetRaftRequestIdFromQuery());
                    var (index, _) = await ServerStore.SendToLeaderAsync(editTimeSeries);

                    await WaitForIndexToBeAppliedAsync(context, index);
                    await SendConfigurationResponseAsync(context, index);
                }
            }
        }

        [RavenAction("/databases/*/timeseries/names/config", "POST", AuthorizationStatus.ValidUser, EndpointType.Write)]
        public async Task ConfigTimeSeriesNames()
        {
            await ServerStore.EnsureNotPassiveAsync();

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (var json = await context.ReadForDiskAsync(RequestBodyStream(), "time-series value names"))
            {
                var parameters = JsonDeserializationServer.Parameters.TimeSeriesValueNamesParameters(json);
                parameters.Validate();

                TimeSeriesConfiguration current;
                using (context.OpenReadTransaction())
                {
                    current = ServerStore.Cluster.ReadRawDatabaseRecord(context, Database.Name).TimeSeriesConfiguration ?? new TimeSeriesConfiguration();
                }

                if (current.NamedValues == null)
                    current.AddValueName(parameters.Collection, parameters.TimeSeries, parameters.ValueNames);
                else
                {
                    var currentNames = current.GetNames(parameters.Collection, parameters.TimeSeries);
                    if (currentNames?.SequenceEqual(parameters.ValueNames, StringComparer.Ordinal) == true)
                        return; // no need to update, they identical

                    if (parameters.Update == false)
                    {
                        if (current.TryAddValueName(parameters.Collection, parameters.TimeSeries, parameters.ValueNames) == false)
                            throw new InvalidOperationException(
                                $"Failed to update the names for time-series '{parameters.TimeSeries}' in collection '{parameters.Collection}', they already exists.");
                    }
                    current.AddValueName(parameters.Collection, parameters.TimeSeries, parameters.ValueNames);
                }
                var editTimeSeries = new EditTimeSeriesConfigurationCommand(current, Database.Name, GetRaftRequestIdFromQuery());
                var (index, _) = await ServerStore.SendToLeaderAsync(editTimeSeries);

                await WaitForIndexToBeAppliedAsync(context, index);
                await SendConfigurationResponseAsync(context, index);
            }
        }

        private async Task SendConfigurationResponseAsync(TransactionOperationContext context, long index)
        {
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                var response = new DynamicJsonValue { ["RaftCommandIndex"] = index, };
                context.Write(writer, response);
            }
        }

        public class ExecuteTimeSeriesBatchCommand : TransactionOperationsMerger.MergedTransactionCommand
        {
            private readonly DocumentDatabase _database;
            private readonly string _documentId;
            private readonly TimeSeriesOperation _operation;
            private readonly bool _fromEtl;

            public string LastChangeVector;

            public ExecuteTimeSeriesBatchCommand(DocumentDatabase database, string documentId, TimeSeriesOperation operation, bool fromEtl)
            {
                _database = database;
                _documentId = documentId;
                _operation = operation;
                _fromEtl = fromEtl;
            }

            protected override long ExecuteCmd(DocumentsOperationContext context)
            {
                string docCollection = GetDocumentCollection(_database, context, _documentId, _fromEtl);

                if (docCollection == null)
                    return 0L;

                var changes = 0L;
                var tss = _database.DocumentsStorage.TimeSeriesStorage;

                if (_operation.Deletes?.Count > 0)
                {
                    foreach (var removal in _operation.Deletes)
                    {
                        var deletionRange = new TimeSeriesStorage.DeletionRangeRequest
                        {
                            DocumentId = _documentId,
                            Collection = docCollection,
                            Name = _operation.Name,
                            From = removal.From ?? DateTime.MinValue,
                            To = removal.To ?? DateTime.MaxValue
                        };

                        LastChangeVector = tss.DeleteTimestampRange(context, deletionRange);

                        changes++;
                    }
                }

                if (_operation.Increments?.Count > 0)
                {
                    LastChangeVector = tss.IncrementTimestamp(context,
                        _documentId,
                        docCollection,
                        _operation.Name,
                        _operation.Increments
                    );

                    changes += _operation.Increments.Count;
                }

                if (_operation.Appends?.Count > 0 == false)
                    return changes;

                LastChangeVector = tss.AppendTimestamp(context,
                    _documentId,
                    docCollection,
                    _operation.Name,
                    _operation.Appends
                );

                changes += _operation.Appends.Count;

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

        private static readonly TimeSeriesStorage.AppendOptions AppendOptionsForSmuggler = new TimeSeriesStorage.AppendOptions
        {
            VerifyName = false,
            FromSmuggler = true
        };

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
                            if (tss.TryAppendEntireSegmentFromSmuggler(context, slicer.TimeSeriesKeySlice, collectionName, item))
                            {
                                // on import we remove all @time-series from the document, so we need to re-add them
                                tss.AddTimeSeriesNameToMetadata(context, item.DocId, item.Name, NonPersistentDocumentFlags.FromSmuggler);
                                continue;
                            }
                        }

                        var values = item.Segment.YieldAllValues(context, context.Allocator, item.Baseline);
                        tss.AppendTimestamp(context, docId, item.Collection, item.Name, values, AppendOptionsForSmuggler);
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

        [RavenAction("/databases/*/timeseries/debug/segments-summary", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task GetSegmentSummary()
        {
            var documentId = GetStringQueryString("docId");
            var name = GetStringQueryString("name");
            var from = GetDateTimeQueryString("from", false) ?? DateTime.MinValue;
            var to = GetDateTimeQueryString("to", false) ?? DateTime.MaxValue;

            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                var segmentsSummary = Database.DocumentsStorage.TimeSeriesStorage.GetSegmentsSummary(context, documentId, name, from, to);

                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartObject();
                    writer.WritePropertyName("Results");
                    writer.WriteStartArray();
                    var first = true;
                    foreach (var summery in segmentsSummary)
                    {
                        if (!first)
                            writer.WriteComma();
                        context.Write(writer, summery.ToJson());
                        first = false;
                    }
                    writer.WriteEndArray();
                    writer.WriteEndObject();
                }
            }
        }
    }
}
