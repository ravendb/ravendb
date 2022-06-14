using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Commands.Batches;
using Raven.Server.Documents.Handlers.Batches;
using Raven.Server.Documents.Handlers.Processors.Batches;
using Raven.Server.Documents.Sharding.Commands;
using Raven.Server.Documents.Sharding.Handlers.Batches;
using Raven.Server.Documents.Sharding.Operations;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Utils;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Batches;

internal class ShardedBatchHandlerProcessorForBulkDocs : AbstractBatchHandlerProcessorForBulkDocs<ShardedBatchCommand, ShardedDatabaseRequestHandler, TransactionOperationContext>
{
    public ShardedBatchHandlerProcessorForBulkDocs([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override async ValueTask<DynamicJsonArray> HandleTransactionAsync(JsonOperationContext context, ShardedBatchCommand command, IndexBatchOptions indexBatchOptions, ReplicationBatchOptions replicationBatchOptions)
    {
        var shardedBatchCommands = new Dictionary<int, ShardedSingleNodeBatchCommand>(); // TODO sharding : consider cache those
        foreach (var c in command)
        {
            var shardNumber = c.ShardNumber;
            if (shardedBatchCommands.TryGetValue(shardNumber, out var shardedBatchCommand) == false)
            {
                shardedBatchCommand = new ShardedSingleNodeBatchCommand(indexBatchOptions, replicationBatchOptions);
                shardedBatchCommands.Add(shardNumber, shardedBatchCommand);
            }
            shardedBatchCommand.AddCommand(c);
        }

        var op = new SingleNodeShardedBatchOperation(HttpContext, context, shardedBatchCommands, command.ParsedCommands.Count);
        return await RequestHandler.ShardExecutor.ExecuteParallelForShardsAsync(shardedBatchCommands.Keys.ToArray(), op);
    }

    protected override ValueTask WaitForIndexesAsync(IndexBatchOptions options, string lastChangeVector, long lastTombstoneEtag,
        HashSet<string> modifiedCollections)
    {
        // no-op
        // this is passed as a parameter when we execute transaction on each shard
        return ValueTask.CompletedTask;
    }

    protected override ValueTask WaitForReplicationAsync(ReplicationBatchOptions options, string lastChangeVector)
    {
        // no-op
        // this is passed as a parameter when we execute transaction on each shard
        return ValueTask.CompletedTask;
    }

    protected override char GetIdentityPartsSeparator() => RequestHandler.DatabaseContext.IdentityPartsSeparator;

    protected override AbstractBatchCommandsReader<ShardedBatchCommand, TransactionOperationContext> GetCommandsReader() => new ShardedBatchCommandsReader(RequestHandler);

    protected override AbstractClusterTransactionRequestProcessor<ShardedDatabaseRequestHandler, ShardedBatchCommand> GetClusterTransactionRequestProcessor() => new ShardedClusterTransactionRequestProcessor(RequestHandler);
}
