using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CsvHelper;
using CsvHelper.Configuration;
using Raven.Client;
using Raven.Client.Exceptions;
using Raven.Client.Util;
using Raven.Server.Documents.Handlers.Processors.Replication;
using Raven.Server.Documents.Replication.ReplicationItems;
using Raven.Server.Documents.Replication.Senders;
using Raven.Server.Documents.Replication.Stats;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers
{
    public sealed class ReplicationHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/replication/tombstones", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task GetAllTombstones()
        {
            using (var processor = new ReplicationHandlerProcessorForGetTombstones(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/replication/conflicts", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task GetReplicationConflicts()
        {
            using (var processor = new ReplicationHandlerProcessorForGetConflicts(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/replication/performance", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task Performance()
        {
            using (var processor = new ReplicationHandlerProcessorForGetPerformance(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/replication/performance/live", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, SkipUsagesCount = true)]
        public async Task PerformanceLive()
        {
            using (var processor = new ReplicationHandlerProcessorForGetPerformanceLive(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/replication/pulses/live", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, SkipUsagesCount = true)]
        public async Task PulsesLive()
        {
            using (var processor = new ReplicationHandlerProcessorForGetPulsesLive(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/replication/active-connections", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task GetReplicationActiveConnections()
        {
            using (var processor = new ReplicationHandlerProcessorForGetActiveConnections(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/replication/debug/outgoing-failures", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, IsDebugInformationEndpoint = true)]
        public async Task GetReplicationOutgoingFailureStats()
        {
            using (var processor = new ReplicationHandlerProcessorForGetOutgoingFailureStats(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/replication/debug/incoming-last-activity-time", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, IsDebugInformationEndpoint = true)]
        public async Task GetReplicationIncomingActivityTimes()
        {
            using (var processor = new ReplicationHandlerProcessorForGetIncomingActivityTimes(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/replication/debug/incoming-rejection-info", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, IsDebugInformationEndpoint = true)]
        public async Task GetReplicationIncomingRejectionInfo()
        {
            using (var processor = new ReplicationHandlerProcessorForGetIncomingRejectionInfo(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/replication/debug/outgoing-reconnect-queue", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, IsDebugInformationEndpoint = true)]
        public async Task GetReplicationReconnectionQueue()
        {
            using (var processor = new ReplicationHandlerProcessorForGetOutgoingReconnectionQueue(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/replication/conflicts/solver", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task GetConflictSolver()
        {
            using (var processor = new ReplicationHandlerProcessorForGetConflictSolver(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/debug/replication/all-items", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task GetAllItems()
        {
            var etag = GetLongQueryString("etag", required: false) ?? 0L;
            var pageSize = GetPageSize();
            var types = GetStringValuesQueryString("type", required: false)
                .Select(x => Enum.Parse<ReplicationBatchItem.ReplicationItemType>(x, ignoreCase: true))
                .ToHashSet();
            var format = (GetStringQueryString("format", required: false) ?? "json").ToLower();
            var columns = GetStringValuesQueryString("column", required: false).ToArray();

            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                var runStats = new OutgoingReplicationRunStats();
                var stats = new ReplicationDocumentSenderBase.ReplicationStats
                {
                    Network = new OutgoingReplicationStatsScope(runStats),
                    Storage = new OutgoingReplicationStatsScope(runStats),
                    AttachmentRead = new OutgoingReplicationStatsScope(runStats),
                    CounterRead = new OutgoingReplicationStatsScope(runStats),
                    DocumentRead = new OutgoingReplicationStatsScope(runStats),
                    TombstoneRead = new OutgoingReplicationStatsScope(runStats),
                    TimeSeriesRead = new OutgoingReplicationStatsScope(runStats),
                };

                var supportedFeatures = new ReplicationDocumentSenderBase.ReplicationSupportedFeatures
                {
                    CaseInsensitiveCounters = true,
                    RevisionTombstonesWithId = true
                };

                var items = ReplicationDocumentSenderBase.GetReplicationItems(Database, context, etag: etag == 0 ? 0 : etag - 1, stats, supportedFeatures);
                if (types.Count > 0)
                    items = items.Where(x => types.Contains(x.Type));
                items = items.Take(pageSize);

                var debugItems = items.Select(x => x.ToDebugJson());

                switch (format)
                {
                    case "json":
                        await WriteJsonReplicationItems(context, debugItems);
                        break;
                    case "csv":
                        await WriteCsvReplicationItems(columns, debugItems);
                        break; 
                    default:
                        throw new BadRequestException($"Unknown format: '{format}'. Supported formats are 'json' and 'csv'.");
                }
            }
        }

        private async Task WriteCsvReplicationItems(string[] columns, IEnumerable<DynamicJsonValue> debugItems)
        {
            if (columns.Length == 0)
                columns =
                [
                    nameof(ReplicationBatchItem.Type), nameof(ReplicationBatchItem.Etag), nameof(ReplicationBatchItem.ChangeVector),
                    nameof(ReplicationBatchItem.LastModifiedTicks), nameof(ReplicationBatchItem.TransactionMarker), nameof(ReplicationBatchItem.Size)
                ];
            var encodedCsvFileName = Uri.EscapeDataString($"replication-all-items_{SystemTime.UtcNow.ToString("yyyyMMdd_HHmm", CultureInfo.InvariantCulture)}.csv");

            HttpContext.Response.Headers.ContentDisposition = $"attachment; filename=\"{encodedCsvFileName}\"; filename*=UTF-8''{encodedCsvFileName}";
            HttpContext.Response.Headers[Constants.Headers.ContentType] = "text/csv";

            await using (var writer = new StreamWriter(HttpContext.Response.Body, Encoding.UTF8))
            await using (var csvWriter = new CsvWriter(writer, new CsvConfiguration(CultureInfo.InvariantCulture) { Delimiter = "," }))
            {
                foreach (string column in columns)
                {
                    csvWriter.WriteField(column);
                }

                await csvWriter.NextRecordAsync();
                foreach (var item in debugItems)
                {
                    foreach (string column in columns)
                    {
                        var value = item[column];
                        csvWriter.WriteField(field: value?.ToString());
                    }

                    await csvWriter.NextRecordAsync();
                }
            }
        }

        private async Task WriteJsonReplicationItems(DocumentsOperationContext context, IEnumerable<DynamicJsonValue> debugItems)
        {
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                context.Write(writer, new DynamicJsonValue
                {
                    ["Results"] = new DynamicJsonArray(debugItems),
                    ["DatabaseChangeVector"] = DocumentsStorage.GetFullDatabaseChangeVector(context)
                });
            }
        }

        [RavenAction("/databases/*/replication/progress", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, IsDebugInformationEndpoint = true)]
        public async Task GetReplicationProgress()
        {
            using (var processor = new ReplicationHandlerProcessorForGetOngoingTasksProgress(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/replication/internal/outgoing/progress", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, IsDebugInformationEndpoint = true)]
        public async Task GetOutgoingInternalReplicationProgress()
        {
            using (var processor = new ReplicationHandlerProcessorForGetOutgoingInternalReplicationProgress(this))
                await processor.ExecuteAsync();
        }
    }
}
