using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Primitives;
using Raven.Client;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Documents.Session.TimeSeries;
using Raven.Client.Exceptions.Documents;
using Raven.Server.Documents.Includes;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Server.Smuggler.Documents;
using Raven.Server.TrafficWatch;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server;

namespace Raven.Server.Documents.Handlers
{
    public class TimeSeriesHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/timeseries/stats", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task Stats()
        {
            var documentId = GetStringQueryString("docId");

            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                var document = Database.DocumentsStorage.Get(context, documentId, DocumentFields.Data);
                if (document == null)
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                    return;
                }

                var timeSeriesNames = GetTimesSeriesNames(document);
                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartObject();

                    writer.WritePropertyName(nameof(TimeSeriesStatistics.DocumentId));
                    writer.WriteString(documentId);
                    writer.WriteComma();

                    writer.WritePropertyName(nameof(TimeSeriesStatistics.TimeSeries));

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

            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                var includesCommand = includeDoc || includeTags
                    ? new IncludeDocumentsDuringTimeSeriesLoadingCommand(context, documentId, includeDoc, includeTags)
                    : null;

                var ranges = GetTimeSeriesRangeResults(context, documentId, names, fromList, toList, start, pageSize, includesCommand);

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
            var documentId = GetStringQueryString("docId");
            var name = GetStringQueryString("name");
            var fromStr = GetStringQueryString("from", required: false);
            var toStr = GetStringQueryString("to", required: false);

            var start = GetStart();
            var pageSize = GetPageSize();

            var includeDoc = GetBoolValueQueryString("includeDocument", required: false) ?? false;
            var includeTags = GetBoolValueQueryString("includeTags", required: false) ?? false;

            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                var from = string.IsNullOrEmpty(fromStr)
                    ? DateTime.MinValue
                    : ParseDate(fromStr, name);

                var to = string.IsNullOrEmpty(toStr)
                    ? DateTime.MaxValue
                    : ParseDate(toStr, name);

                var stats = context.DocumentDatabase.DocumentsStorage.TimeSeriesStorage.Stats.GetStats(context, documentId, name);
                if (stats == default)
                {
                    // non existing time series
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                    return;
                }

                var includesCommand = includeDoc || includeTags
                    ? new IncludeDocumentsDuringTimeSeriesLoadingCommand(context, documentId, includeDoc, includeTags)
                    : null;

                var rangeResult = GetTimeSeriesRange(context, documentId, name, from, to, ref start, ref pageSize, includesCommand);
                var hash = rangeResult?.Hash ?? string.Empty;

                var etag = GetStringFromHeaders("If-None-Match");
                if (etag == hash)
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.NotModified;
                    return;
                }

                HttpContext.Response.Headers[Constants.Headers.Etag] = "\"" + hash + "\"";

                long? totalCount = null;
                if (from <= stats.Start && to >= stats.End)
                {
                    totalCount = stats.Count;
                }

                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    if (rangeResult != null)
                        WriteRange(writer, rangeResult, totalCount);
                }
            }
        }

        private static Dictionary<string, List<TimeSeriesRangeResult>> GetTimeSeriesRangeResults(DocumentsOperationContext context, string documentId, StringValues names, StringValues fromList, StringValues toList, int start, int pageSize, IncludeDocumentsDuringTimeSeriesLoadingCommand includes)
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

                var from = string.IsNullOrEmpty(fromList[i]) ? DateTime.MinValue : ParseDate(fromList[i], name);
                var to = string.IsNullOrEmpty(toList[i]) ? DateTime.MaxValue : ParseDate(toList[i], name);

                var rangeResult = GetTimeSeriesRange(context, documentId, name, from, to, ref start, ref pageSize, includes);
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

        internal static unsafe TimeSeriesRangeResult GetTimeSeriesRange(DocumentsOperationContext context, string docId, string name, DateTime from, DateTime to, ref int start, ref int pageSize,
            IncludeDocumentsDuringTimeSeriesLoadingCommand includesCommand = null)
        {
            if (pageSize == 0)
                return null;

            List<TimeSeriesEntry> values = new List<TimeSeriesEntry>();

            var reader = new TimeSeriesReader(context, docId, name, from, to, offset: null);

            // init hash
            var size = Sodium.crypto_generichash_bytes();
            Debug.Assert((int)size == 32);
            var cryptoGenerichashStatebytes = (int)Sodium.crypto_generichash_statebytes();
            var state = stackalloc byte[cryptoGenerichashStatebytes];
            if (Sodium.crypto_generichash_init(state, null, UIntPtr.Zero, size) != 0)
                ComputeHttpEtags.ThrowFailToInitHash();

            var initialStart = start;
            var hasMore = false;
            DateTime lastSeenEntry = from;

            includesCommand?.InitializeNewRangeResult(state);

            foreach (var (individualValues, segmentResult) in reader.SegmentsOrValues())
            {
                if (individualValues == null &&
                    start > segmentResult.Summary.NumberOfLiveEntries)
                {
                    lastSeenEntry = segmentResult.End;
                    start -= segmentResult.Summary.NumberOfLiveEntries;
                    continue;
                }

                var enumerable = individualValues ?? segmentResult.Values;

                foreach (var singleResult in enumerable)
                {
                    lastSeenEntry = segmentResult.End;

                    if (start-- > 0)
                        continue;

                    if (pageSize-- <= 0)
                    {
                        hasMore = true;
                        break;
                    }

                    includesCommand?.Fill(singleResult.Tag);

                    values.Add(new TimeSeriesEntry
                    {
                        Timestamp = singleResult.Timestamp,
                        Tag = singleResult.Tag,
                        Values = singleResult.Values.ToArray(),
                        IsRollup = singleResult.Type == SingleResultType.RolledUp
                    });
                }

                ComputeHttpEtags.HashChangeVector(state, segmentResult.ChangeVector);

                if (pageSize <= 0)
                    break;
            }

            var hash = ComputeHttpEtags.FinalizeHash(size, state);

            TimeSeriesRangeResult result;

            if (initialStart > 0 && values.Count == 0)
            {
                // this is a special case, because before the 'start' we might have values
                result = new TimeSeriesRangeResult
                {
                    From = lastSeenEntry,
                    To = to,
                    Entries = values.ToArray(),
                    Hash = hash
                };
            }
            else
            {
                result = new TimeSeriesRangeResult
                {
                    From = (initialStart > 0) ? values[0].Timestamp : from,
                    To = hasMore ? values.Last().Timestamp : to,
                    Entries = values.ToArray(),
                    Hash = hash
                };
            }

            includesCommand?.AddIncludesToResult(result);

            return result;
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

                    WriteRange(writer, ranges[i], totalCount);

                    await writer.MaybeFlushAsync(token);
                }
                writer.WriteEndArray();
            }

            writer.WriteEndObject();
        }

        internal static void WriteTimeSeriesRangeResults(DocumentsOperationContext context, AsyncBlittableJsonTextWriter writer, string documentId, Dictionary<string, List<TimeSeriesRangeResult>> dictionary)
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

                    WriteRange(writer, ranges[i], totalCount);
                }
                writer.WriteEndArray();
            }

            writer.WriteEndObject();
        }

        private static void WriteRange(AsyncBlittableJsonTextWriter writer, TimeSeriesRangeResult rangeResult, long? totalCount)
        {
            writer.WriteStartObject();
            {
                writer.WritePropertyName(nameof(TimeSeriesRangeResult.From));
                if (rangeResult.From == DateTime.MinValue)
                    writer.WriteNull();
                else
                    writer.WriteDateTime(rangeResult.From, true);
                writer.WriteComma();

                writer.WritePropertyName(nameof(TimeSeriesRangeResult.To));
                if (rangeResult.To == DateTime.MaxValue)
                    writer.WriteNull();
                else
                    writer.WriteDateTime(rangeResult.To, true);
                writer.WriteComma();

                writer.WritePropertyName(nameof(TimeSeriesRangeResult.Entries));
                WriteEntries(writer, rangeResult.Entries);

                if (totalCount.HasValue)
                {
                    // add total entries count to the response
                    writer.WriteComma();
                    writer.WritePropertyName(nameof(TimeSeriesRangeResult.TotalResults));
                    writer.WriteInteger(totalCount.Value);
                }

                if (rangeResult.Includes != null)
                {
                    // add included documents to the response
                    writer.WriteComma();
                    writer.WritePropertyName(nameof(TimeSeriesRangeResult.Includes));
                    writer.WriteObject(rangeResult.Includes);
                }
            }
            writer.WriteEndObject();
        }

        private static void WriteEntries(AsyncBlittableJsonTextWriter writer, TimeSeriesEntry[] entries)
        {
            writer.WriteStartArray();

            for (var i = 0; i < entries.Length; i++)
            {
                if (i > 0)
                    writer.WriteComma();

                writer.WriteStartObject();
                {
                    writer.WritePropertyName(nameof(TimeSeriesEntry.Timestamp));
                    writer.WriteDateTime(entries[i].Timestamp, true);
                    writer.WriteComma();
                    writer.WritePropertyName(nameof(TimeSeriesEntry.Tag));
                    writer.WriteString(entries[i].Tag);
                    writer.WriteComma();
                    writer.WriteArray(nameof(TimeSeriesEntry.Values), entries[i].Values);
                    writer.WriteComma();
                    writer.WritePropertyName(nameof(TimeSeriesEntry.IsRollup));
                    writer.WriteBool(entries[i].IsRollup);
                }
                writer.WriteEndObject();
            }

            writer.WriteEndArray();
        }

        [RavenAction("/databases/*/timeseries", "POST", AuthorizationStatus.ValidUser, EndpointType.Write)]
        public async Task Batch()
        {
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                var documentId = GetQueryStringValueAndAssertIfSingleAndNotEmpty("docId");

                var blittable = await context.ReadForMemoryAsync(RequestBodyStream(), "timeseries");
                var operation = TimeSeriesOperation.Parse(blittable);

                if (TrafficWatchManager.HasRegisteredClients)
                    AddStringToHttpContext(blittable.ToString(), TrafficWatchChangeType.TimeSeries);

                var cmd = new ExecuteTimeSeriesBatchCommand(Database, documentId, operation, false);

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

        [RavenAction("/databases/*/timeseries/config", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task GetTimeSeriesConfig()
        {
            using (Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                TimeSeriesConfiguration timeSeriesConfig;
                using (var rawRecord = Server.ServerStore.Cluster.ReadRawDatabaseRecord(context, Database.Name))
                {
                    timeSeriesConfig = rawRecord?.TimeSeriesConfiguration;
                }

                if (timeSeriesConfig != null)
                {
                    await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                    {
                        context.Write(writer, timeSeriesConfig.ToJson());
                    }
                }
                else
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
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
                    if (configuration == null)
                    {
                        return;
                    }

                    var hasCollectionsConfig = configuration.TryGet(nameof(TimeSeriesConfiguration.Collections), out BlittableJsonReaderObject collections) &&
                                               collections?.Count > 0;

                    if (hasCollectionsConfig == false)
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

                await WaitForIndexToBeApplied(context, index);
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

                    await WaitForIndexToBeApplied(context, index);
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

                await WaitForIndexToBeApplied(context, index);
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
                                tss.AddTimeSeriesNameToMetadata(context, item.DocId, item.Name);
                                continue;
                            }
                        }

                        var values = item.Segment.YieldAllValues(context, context.Allocator, item.Baseline);
                        tss.AppendTimestamp(context, docId, item.Collection, item.Name, values, verifyName: false);
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
