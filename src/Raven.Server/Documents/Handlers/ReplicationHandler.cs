using System.Linq;
using System.Threading.Tasks;
using Raven.Server.Documents.Handlers.Processors.Replication;
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

            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
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

                var items = ReplicationDocumentSenderBase.GetReplicationItems(Database, context, etag, stats, supportedFeatures)
                    .Take(pageSize);

                context.Write(writer, new DynamicJsonValue
                {
                    ["Results"] = new DynamicJsonArray(items.Select(x => x.ToDebugJson()))
                });
            }
        }


        [RavenAction("/databases/*/replication/progress", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, IsDebugInformationEndpoint = true)]
        public async Task GetReplicationProgress()
        {
            using (var processor = new ReplicationHandlerProcessorForGetOngoingTasksProgress(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/outgoing-internal-replication/progress", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, IsDebugInformationEndpoint = true)]
        public async Task GetOutgoingInternalReplicationProgress()
        {
            using (var processor = new ReplicationHandlerProcessorForGetOutgoingInternalReplicationProgress(this))
                await processor.ExecuteAsync();
        }
    }
}
