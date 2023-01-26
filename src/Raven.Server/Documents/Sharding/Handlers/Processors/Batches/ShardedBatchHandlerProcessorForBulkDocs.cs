using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions.Sharding;
using Raven.Server.Documents.Handlers.Batches;
using Raven.Server.Documents.Handlers.Processors.Batches;
using Raven.Server.Documents.Sharding.Commands;
using Raven.Server.Documents.Sharding.Handlers.Batches;
using Raven.Server.Documents.Sharding.Operations;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Batches;

internal class ShardedBatchHandlerProcessorForBulkDocs : AbstractBatchHandlerProcessorForBulkDocs<ShardedBatchCommand, ShardedDatabaseRequestHandler, TransactionOperationContext>
{
    public ShardedBatchHandlerProcessorForBulkDocs([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override async ValueTask<DynamicJsonArray> HandleTransactionAsync(JsonOperationContext context, ShardedBatchCommand command, IndexBatchOptions indexBatchOptions, ReplicationBatchOptions replicationBatchOptions)
    {
        var batchBehavior = GetBatchBehavior();

        var retries = 5;
        object[] result = null;
        while (true)
        {
            try
            {
                var resultSize = 0;
                var batchPerShard = new Dictionary<int, ShardedSingleNodeBatchCommand>(); // TODO sharding : consider cache those
                foreach (var c in command.GetCommands(batchBehavior))
                {
                    var shardNumber = c.ShardNumber;
                    if (batchPerShard.TryGetValue(shardNumber, out var shardedBatchCommand) == false)
                    {
                        shardedBatchCommand = new ShardedSingleNodeBatchCommand(shardNumber, indexBatchOptions, replicationBatchOptions);
                        batchPerShard.Add(shardNumber, shardedBatchCommand);
                    }

                    shardedBatchCommand.AddCommand(c);
                    resultSize++;
                }

                result ??= new object[resultSize];
                var op = new SingleNodeShardedBatchOperation(HttpContext, context, batchPerShard, command, result);

                var error = await RequestHandler.ShardExecutor.ExecuteParallelAndIgnoreErrorsForShardsAsync(batchPerShard.Keys.ToArray(), op);
                if (error == null)
                    return new DynamicJsonArray(result);

                throw error;
            }
            catch (WrongShardException) when (retries > 0)
            {
                retries--;
                await Task.Delay(100);
            }
        }
    }

    protected override ValueTask WaitForIndexesAsync(IndexBatchOptions options, string lastChangeVector, long lastTombstoneEtag,
        HashSet<string> modifiedCollections)
    {
        // no-op
        // this is passed as a parameter when we execute transaction on each shard
        return ValueTask.CompletedTask;
    }

    protected override ValueTask WaitForReplicationAsync(TransactionOperationContext context, ReplicationBatchOptions options, string lastChangeVector)
    {
        // no-op
        // this is passed as a parameter when we execute transaction on each shard
        return ValueTask.CompletedTask;
    }

    private ShardedBatchBehavior GetBatchBehavior()
    {
        var shardedBatchBehaviorAsString = RequestHandler.GetStringQueryString("shardedBatchBehavior", required: false);
        if (shardedBatchBehaviorAsString == null)
            return ShardedBatchBehavior.NonTransactionalMultiBucket;

        if (Enum.TryParse<ShardedBatchBehavior>(shardedBatchBehaviorAsString, ignoreCase: true, out var shardedBatchBehavior) == false)
            throw new InvalidOperationException($"Invalid sharded batch behavior value '{shardedBatchBehaviorAsString}'.");

        if (shardedBatchBehavior == ShardedBatchBehavior.Default)
            return ShardedBatchBehavior.NonTransactionalMultiBucket;

        return shardedBatchBehavior;

    }

    protected override char GetIdentityPartsSeparator() => RequestHandler.DatabaseContext.IdentityPartsSeparator;

    protected override AbstractBatchCommandsReader<ShardedBatchCommand, TransactionOperationContext> GetCommandsReader() => new ShardedBatchCommandsReader(RequestHandler);

    protected override AbstractClusterTransactionRequestProcessor<ShardedDatabaseRequestHandler, ShardedBatchCommand> GetClusterTransactionRequestProcessor() => new ShardedClusterTransactionRequestProcessor(RequestHandler);
}
