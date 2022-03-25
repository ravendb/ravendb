using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Handlers;
using Raven.Server.Documents.Handlers.Processors.Batches;
using Raven.Server.ServerWide.Context;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Batches;

internal class ShardedBatchHandlerProcessorForBulkDocs : AbstractBatchHandlerProcessorForBulkDocs<ShardedBatchCommand, ShardedDatabaseRequestHandler, TransactionOperationContext>
{
    public ShardedBatchHandlerProcessorForBulkDocs([NotNull] ShardedDatabaseRequestHandler requestHandler)
        : base(requestHandler, requestHandler.ContextPool)
    {
    }

    protected override async ValueTask<DynamicJsonArray> HandleTransactionAsync(ShardedBatchCommand command)
    {
        var shardedBatchCommands = new Dictionary<int, SingleNodeShardedBatchCommand>(); // TODO sharding : consider cache those
        foreach (var c in command)
        {
            var shardNumber = c.ShardNumber;
            var requestExecutor = RequestHandler.DatabaseContext.RequestExecutors[shardNumber];

            if (shardedBatchCommands.TryGetValue(shardNumber, out var shardedBatchCommand) == false)
            {
                shardedBatchCommand = new SingleNodeShardedBatchCommand(RequestHandler, requestExecutor.ContextPool);
                shardedBatchCommands.Add(shardNumber, shardedBatchCommand);
            }

            shardedBatchCommand.AddCommand(c);
        }

        var tasks = new List<Task>();
        foreach (var c in shardedBatchCommands)
        {
            tasks.Add(RequestHandler.DatabaseContext.RequestExecutors[c.Key].ExecuteAsync(c.Value, c.Value.Context));
        }

        await Task.WhenAll(tasks);

        var reply = new object[command.ParsedCommands.Count];
        foreach (var c in shardedBatchCommands.Values)
            c.AssembleShardedReply(reply);

        return new DynamicJsonArray(reply);
    }

    protected override ValueTask WaitForIndexesAsync(TimeSpan timeout, List<string> specifiedIndexesQueryString, bool throwOnTimeout, string lastChangeVector, long lastTombstoneEtag,
        HashSet<string> modifiedCollections)
    {
        throw new NotImplementedException();
    }

    protected override ValueTask WaitForReplicationAsync(TimeSpan waitForReplicasTimeout, string numberOfReplicasStr, bool throwOnTimeoutInWaitForReplicas, string lastChangeVector)
    {
        throw new NotImplementedException();
    }

    protected override char GetIdentityPartsSeparator() => RequestHandler.DatabaseContext.IdentityPartsSeparator;

    protected override BatchRequestParser.AbstractBatchCommandBuilder<ShardedBatchCommand, TransactionOperationContext> GetCommandBuilder() => new ShardedBatchCommandBuilder(RequestHandler);

    protected override AbstractClusterTransactionRequestProcessor<ShardedDatabaseRequestHandler, ShardedBatchCommand> GetClusterTransactionRequestProcessor() => new ShardedClusterTransactionRequestProcessor(RequestHandler);
}
