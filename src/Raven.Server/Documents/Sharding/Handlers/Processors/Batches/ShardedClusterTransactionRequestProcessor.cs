using System;
using System.Threading.Tasks;
using Raven.Server.Config.Categories;
using Raven.Server.Documents.Handlers.Batches;
using Raven.Server.Documents.Handlers.Processors.Batches;
using Raven.Server.Documents.Sharding.Handlers.Batches;
using Raven.Server.Rachis;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Voron;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Batches;

public sealed class ShardedClusterTransactionRequestProcessor : AbstractClusterTransactionRequestProcessor<ShardedDatabaseRequestHandler, ShardedBatchCommand>
{
    public ShardedClusterTransactionRequestProcessor(ShardedDatabaseRequestHandler requestHandler)
        : base(requestHandler)
    {
    }

    protected override ArraySegment<BatchRequestParser.CommandData> GetParsedCommands(ShardedBatchCommand command) => command.ParsedCommands;
    
    protected override ClusterConfiguration GetClusterConfiguration() => RequestHandler.DatabaseContext.Configuration.Cluster;
    public override AsyncWaiter<long?>.RemoveTask CreateClusterTransactionTask(string id, long index, out Task<long?> task)
    {
        return RequestHandler.ServerStore.Cluster.ClusterTransactionWaiter.CreateTask(id, out task);
    }

    public override Task<long?> WaitForDatabaseCompletion(Task<long?> onDatabaseCompletionTask)
    {
        if (onDatabaseCompletionTask.IsCompletedSuccessfully)
            return onDatabaseCompletionTask;

        var t = new Task<long?>(() => null);
        t.Start();

        return t;
    }

    protected override DateTime GetUtcNow()
    {
        return RequestHandler.DatabaseContext.Time.GetUtcNow();
    }

    protected override (string DatabaseGroupId, string ClusterTransactionId) GetDatabaseGroupIdAndClusterTransactionId(TransactionOperationContext ctx, string id)
    {
        var shardNumber = ShardHelper.GetShardNumberFor(RequestHandler.DatabaseContext.DatabaseRecord.Sharding, ctx.Allocator, id);
        var shard = RequestHandler.DatabaseContext.DatabaseRecord.Sharding.Shards[shardNumber];
        return (shard.DatabaseTopologyIdBase64, shard.ClusterTransactionIdBase64);
    }

    protected override ClusterTransactionCommand CreateClusterTransactionCommand(
        ArraySegment<BatchRequestParser.CommandData> parsedCommands,
        ClusterTransactionCommand.ClusterTransactionOptions options,
        string raftRequestId)
    {
        return new ClusterTransactionCommand(
            RequestHandler.DatabaseContext.DatabaseName,
            RequestHandler.DatabaseContext.IdentityPartsSeparator,
            parsedCommands,
            options,
            raftRequestId);
    }
}
