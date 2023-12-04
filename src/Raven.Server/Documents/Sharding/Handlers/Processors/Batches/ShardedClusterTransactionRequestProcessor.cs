using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Extensions;
using Raven.Server.Config.Categories;
using Raven.Server.Documents.Handlers.Batches;
using Raven.Server.Documents.Handlers.Processors.Batches;
using Raven.Server.Documents.Sharding.Handlers.Batches;
using Raven.Server.Rachis;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json.Parsing;
using Voron;
using static Raven.Server.ServerWide.Commands.ClusterTransactionCommand;

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

    public override Task<long?> WaitForDatabaseCompletion(Task<long?> onDatabaseCompletionTask, CancellationToken token)
    {
        if (onDatabaseCompletionTask.IsCompletedSuccessfully)
            return onDatabaseCompletionTask.WithCancellation(token);

        return Task.FromResult<long?>(null).WithCancellation(token);
    }

    protected override DateTime GetUtcNow()
    {
        return RequestHandler.DatabaseContext.Time.GetUtcNow();
    }

    protected override void GenerateDatabaseCommandsEvaluatedResults(List<ClusterTransactionDataCommand> databaseCommands,
        long index, long count, DateTime lastModified, bool? disableAtomicDocumentWrites,
        DynamicJsonArray commandsResults)
    {
        if (count < 0)
            throw new InvalidOperationException($"ClusterTransactionCommand result is invalid - count lower then 0 ({count}).");

        using (RequestHandler.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
        {
            var changeVectors = GetChangeVectors(ctx, databaseCommands, index, count, disableAtomicDocumentWrites);

            foreach (var dataCmd in databaseCommands)
            {
                var cv = changeVectors[dataCmd.Id];
                commandsResults.Add(GetCommandResultJson(dataCmd, cv, lastModified));
            }
        }
    }

    private Dictionary<string, string> GetChangeVectors(TransactionOperationContext ctx, List<ClusterTransactionDataCommand> databaseCommands,
        long index, long count, bool? disableAtomicDocumentWrites)
    {
        var shardsOrder = new List<int>();
        var shardsCmds = new Dictionary<int, List<(string Id, string DatabaseGroupId, string ClusterTransactionId)>>();

        foreach (var dataCmd in databaseCommands)
        {
            var id = dataCmd.Id;
            var (databaseGroupId, clusterTransactionId, s) = GetDataCommandInfo(ctx, id);

            if (shardsOrder.Contains(s) == false)
            {
                shardsOrder.Add(s);
                shardsCmds.Add(s, new List<(string Id, string DatabaseGroupId, string ClusterTransactionId)>());
            }

            shardsCmds[s].Add((id, databaseGroupId, clusterTransactionId));
        }

        var changeVectors = new Dictionary<string, string>(); // key: cmdsId, val: count
        foreach (var shard in shardsOrder)
        {
            var cmds = shardsCmds[shard];
            foreach (var (id, databaseGroupId, clusterTransactionId) in cmds)
            {
                count++;
                var cv = GenerateChangeVector(index, count, disableAtomicDocumentWrites, databaseGroupId, clusterTransactionId);
                changeVectors.Add(id, cv);
            }
        }

        return changeVectors;
    }

    private (string DatabaseGroupId, string ClusterTransactionId, int ShardNumber) GetDataCommandInfo(TransactionOperationContext ctx, string id)
    {
        var shardNumber = ShardHelper.GetShardNumberFor(RequestHandler.DatabaseContext.DatabaseRecord.Sharding, ctx.Allocator, id);
        var shard = RequestHandler.DatabaseContext.DatabaseRecord.Sharding.Shards[shardNumber];
        return (shard.DatabaseTopologyIdBase64, shard.ClusterTransactionIdBase64, shardNumber);
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
