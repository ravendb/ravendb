using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Batches;
using Raven.Server.Documents.Handlers.Processors.Batches;
using Raven.Server.Documents.Sharding.Handlers.Batches;
using Raven.Server.Documents.Sharding.Operations;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Utils;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Batches;

internal class ShardedBatchHandlerProcessorForBulkDocs : AbstractBatchHandlerProcessorForBulkDocs<ShardedBatchCommand, ShardedDatabaseRequestHandler, TransactionOperationContext>
{
    public ShardedBatchHandlerProcessorForBulkDocs([NotNull] ShardedDatabaseRequestHandler requestHandler)
        : base(requestHandler, requestHandler.ContextPool)
    {
    }

    protected override async ValueTask<DynamicJsonArray> HandleTransactionAsync(JsonOperationContext context, ShardedBatchCommand command)
    {
        var shardedBatchCommands = new Dictionary<int, SingleNodeShardedBatchCommand>(); // TODO sharding : consider cache those
        foreach (var c in command)
        {
            var shardNumber = c.ShardNumber;
            if (shardedBatchCommands.TryGetValue(shardNumber, out var shardedBatchCommand) == false)
            {
                shardedBatchCommand = new SingleNodeShardedBatchCommand(RequestHandler);
                shardedBatchCommands.Add(shardNumber, shardedBatchCommand);
            }
            shardedBatchCommand.AddCommand(c);
        }

        var op = new SingleNodeShardedBatchOperation(HttpContext, context, shardedBatchCommands, command.ParsedCommands.Count);
        return await RequestHandler.ShardExecutor.ExecuteParallelForShardsAsync(shardedBatchCommands.Keys.ToArray(), op);
    }

    protected override ValueTask WaitForIndexesAsync(TimeSpan timeout, List<string> specifiedIndexesQueryString, bool throwOnTimeout, string lastChangeVector, long lastTombstoneEtag,
        HashSet<string> modifiedCollections)
    {
        DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Karmel, DevelopmentHelper.Severity.Normal, "Implement WaitForIndexesAsync");
        throw new NotImplementedException();
    }

    protected override ValueTask WaitForReplicationAsync(TimeSpan waitForReplicasTimeout, string numberOfReplicasStr, bool throwOnTimeoutInWaitForReplicas, string lastChangeVector)
    {
        DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Karmel, DevelopmentHelper.Severity.Normal, "Implement WaitForReplicationAsync");
        throw new NotImplementedException();
    }

    protected override char GetIdentityPartsSeparator() => RequestHandler.DatabaseContext.IdentityPartsSeparator;

    protected override AbstractBatchCommandsReader<ShardedBatchCommand, TransactionOperationContext> GetCommandsReader() => new ShardedBatchCommandsReader(RequestHandler);

    protected override AbstractClusterTransactionRequestProcessor<ShardedDatabaseRequestHandler, ShardedBatchCommand> GetClusterTransactionRequestProcessor() => new ShardedClusterTransactionRequestProcessor(RequestHandler);
}
