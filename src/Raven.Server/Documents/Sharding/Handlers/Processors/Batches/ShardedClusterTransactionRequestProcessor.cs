using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.Config.Categories;
using Raven.Server.Documents.Handlers.Batches;
using Raven.Server.Documents.Handlers.Processors.Batches;
using Raven.Server.Documents.Sharding.Handlers.Batches;
using Raven.Server.Documents.Sharding.Operations;
using Raven.Server.Rachis;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands;
using Sparrow.Json.Parsing;
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

    public override Task WaitForDatabaseCompletion(Task<HashSet<string>> onDatabaseCompletionTask, long index, ClusterTransactionOptions options, CancellationToken token)
    {
        var op = new ShardedWaitForIndexNotificationOperation(RequestHandler, index);
        return RequestHandler.ShardExecutor.ExecuteParallelForAllAsync(op, token);
    }

    protected override ClusterTransactionCommand CreateClusterTransactionCommand(
        ArraySegment<BatchRequestParser.CommandData> parsedCommands,
        ClusterTransactionOptions options,
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
