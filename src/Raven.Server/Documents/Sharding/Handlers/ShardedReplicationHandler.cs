using System.Threading.Tasks;
using Raven.Server.Documents.Sharding.Handlers.Processors.Replication;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Sharding.Handlers;

public sealed class ShardedReplicationHandler : ShardedDatabaseRequestHandler
{
    [RavenShardedAction("/databases/*/replication/conflicts/solver", "GET")]
    public async Task GetConflictSolver()
    {
        using (var processor = new ShardedReplicationHandlerProcessorForGetConflictSolver(this))
            await processor.ExecuteAsync();
    }

    [RavenShardedAction("/databases/*/replication/conflicts", "GET")]
    public async Task GetReplicationConflicts()
    {
        using (var processor = new ShardedReplicationHandlerProcessorForGetConflicts(this))
            await processor.ExecuteAsync();
    }

    [RavenShardedAction("/databases/*/replication/tombstones", "GET")]
    public async Task GetAllTombstones()
    {
        using (var processor = new ShardedReplicationHandlerProcessorForGetTombstones(this))
            await processor.ExecuteAsync();
    }

    [RavenShardedAction("/databases/*/replication/performance", "GET")]
    public async Task Performance()
    {
        using (var processor = new ShardedReplicationHandlerProcessorForGetPerformance(this))
            await processor.ExecuteAsync();
    }

    [RavenShardedAction("/databases/*/replication/active-connections", "GET")]
    public async Task GetReplicationActiveConnections()
    {
        using (var processor = new ShardedReplicationHandlerProcessorForGetActiveConnections(this))
            await processor.ExecuteAsync();
    }

    [RavenShardedAction("/databases/*/replication/performance/live", "GET")]
    public async Task PerformanceLive()
    {
        using (var processor = new ShardedReplicationHandlerProcessorForGetPerformanceLive(this))
            await processor.ExecuteAsync();
    }

    [RavenShardedAction("/databases/*/replication/pulses/live", "GET")]
    public async Task PulsesLive()
    {
        using (var processor = new ShardedReplicationHandlerProcessorForGetPulsesLive(this))
            await processor.ExecuteAsync();
    }

    [RavenShardedAction("/databases/*/replication/debug/outgoing-failures", "GET")]
    public async Task GetReplicationOutgoingFailureStats()
    {
        using (var processor = new ShardedReplicationHandlerProcessorForGetOutgoingFailureStats(this))
            await processor.ExecuteAsync();
    }

    [RavenShardedAction("/databases/*/replication/debug/incoming-last-activity-time", "GET")]
    public async Task GetReplicationIncomingActivityTimes()
    {
        using (var processor = new ShardedReplicationHandlerProcessorForGetIncomingActivityTimes(this))
            await processor.ExecuteAsync();
    }

    [RavenShardedAction("/databases/*/replication/debug/incoming-rejection-info", "GET")]
    public async Task GetReplicationIncomingRejectionInfo()
    {
        using (var processor = new ShardedReplicationHandlerProcessorForGetIncomingRejectionInfo(this))
            await processor.ExecuteAsync();
    }

    [RavenShardedAction("/databases/*/replication/debug/outgoing-reconnect-queue", "GET")]
    public async Task GetReplicationReconnectionQueue()
    {
        using (var processor = new ShardedReplicationHandlerProcessorForGetOutgoingReconnectionQueue(this))
            await processor.ExecuteAsync();
    }

    [RavenShardedAction("/databases/*/replication/progress", "GET")]
    public async Task GetReplicationProgress()
    {
        using (var processor = new ShardedReplicationHandlerProcessorForProgress(this))
            await processor.ExecuteAsync();
    }
}
