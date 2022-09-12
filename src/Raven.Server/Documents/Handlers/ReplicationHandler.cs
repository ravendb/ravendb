using System.Linq;
using System.Threading.Tasks;
using Raven.Server.Documents.Handlers.Processors.Replication;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers
{
    public class ReplicationHandler : DatabaseRequestHandler
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
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriterForDebug(context, ServerStore, ResponseBodyStream()))
            {
                var data = new DynamicJsonArray();
                foreach (var queueItem in Database.ReplicationLoader.ReconnectQueue)
                {
                    data.Add(new DynamicJsonValue
                    {
                        ["Url"] = queueItem.Url,
                        ["Database"] = queueItem.Database,
                        ["Disabled"] = queueItem.Disabled
                    });
                }

                context.Write(writer, new DynamicJsonValue
                {
                    ["Queue-Info"] = data
                });
            }
        }

        [RavenAction("/databases/*/replication/conflicts/solver", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task GetConflictSolver()
        {
            using (var processor = new ReplicationHandlerProcessorForGetConflictSolver(this))
                await processor.ExecuteAsync();
        }
    }
}
