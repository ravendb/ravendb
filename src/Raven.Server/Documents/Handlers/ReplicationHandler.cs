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
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriterForDebug(context, ServerStore, ResponseBodyStream()))
            {
                var data = new DynamicJsonArray();
                foreach (var item in Database.ReplicationLoader.OutgoingFailureInfo)
                {
                    data.Add(new DynamicJsonValue
                    {
                        ["Key"] = new DynamicJsonValue
                        {
                            [nameof(item.Key)] = item.Key.GetType().ToString(),
                            [nameof(item.Key.Url)] = item.Key.Url,
                            [nameof(item.Key.Database)] = item.Key.Database,
                            [nameof(item.Key.Disabled)] = item.Key.Disabled
                        },
                        ["Value"] = new DynamicJsonValue
                        {
                            ["ErrorsCount"] = item.Value.Errors.Count,
                            [nameof(item.Value.Errors)] = new DynamicJsonArray(item.Value.Errors.Select(e => e.ToString())),
                            [nameof(item.Value.NextTimeout)] = item.Value.NextTimeout,
                            [nameof(item.Value.RetryOn)] = item.Value.RetryOn,
                            [nameof(item.Value.DestinationDbId)] = item.Value.DestinationDbId,
                            [nameof(item.Value.LastHeartbeatTicks)] = item.Value.LastHeartbeatTicks,
                        }
                    });
                }

                context.Write(writer, new DynamicJsonValue
                {
                    ["Stats"] = data
                });
            }
        }

        [RavenAction("/databases/*/replication/debug/incoming-last-activity-time", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, IsDebugInformationEndpoint = true)]
        public async Task GetReplicationIncomingActivityTimes()
        {
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriterForDebug(context, ServerStore, ResponseBodyStream()))
            {
                var data = new DynamicJsonArray();
                foreach (var item in Database.ReplicationLoader.IncomingLastActivityTime)
                {
                    data.Add(new DynamicJsonValue
                    {
                        ["Key"] = new DynamicJsonValue
                        {
                            ["SourceDatabaseId"] = item.Key.SourceDatabaseId,
                            ["SourceDatabaseName"] = item.Key.SourceDatabaseName,
                            ["SourceMachineName"] = item.Key.SourceMachineName,
                            ["SourceUrl"] = item.Key.SourceUrl
                        },
                        ["Value"] = item.Value
                    });
                }

                context.Write(writer, new DynamicJsonValue
                {
                    ["Stats"] = data
                });
            }
        }

        [RavenAction("/databases/*/replication/debug/incoming-rejection-info", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, IsDebugInformationEndpoint = true)]
        public async Task GetReplicationIncomingRejectionInfo()
        {
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriterForDebug(context, ServerStore, ResponseBodyStream()))
            {
                var stats = new DynamicJsonArray();
                foreach (var statItem in Database.ReplicationLoader.IncomingRejectionStats)
                {
                    stats.Add(new DynamicJsonValue
                    {
                        ["Key"] = new DynamicJsonValue
                        {
                            ["SourceDatabaseId"] = statItem.Key.SourceDatabaseId,
                            ["SourceDatabaseName"] = statItem.Key.SourceDatabaseName,
                            ["SourceMachineName"] = statItem.Key.SourceMachineName,
                            ["SourceUrl"] = statItem.Key.SourceUrl
                        },
                        ["Value"] = new DynamicJsonArray(statItem.Value.Select(x => new DynamicJsonValue
                        {
                            ["Reason"] = x.Reason,
                            ["When"] = x.When
                        }))
                    });
                }

                context.Write(writer, new DynamicJsonValue
                {
                    ["Stats"] = stats
                });
            }
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
