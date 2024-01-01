using System;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.Config.Categories;
using Raven.Server.Documents.Handlers.Batches;
using Raven.Server.Documents.Handlers.Processors.Batches;
using Raven.Server.Documents.Sharding.Handlers.Batches;
using Raven.Server.Rachis;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Batches;

public sealed class ShardedClusterTransactionRequestProcessor : AbstractClusterTransactionRequestProcessor<ShardedDatabaseRequestHandler, ShardedBatchCommand>
{
    public ShardedClusterTransactionRequestProcessor(ShardedDatabaseRequestHandler requestHandler)
        : base(requestHandler)
    {
    }

    protected override ArraySegment<BatchRequestParser.CommandData> GetParsedCommands(ShardedBatchCommand command) => command.ParsedCommands;

    protected override ClusterConfiguration GetClusterConfiguration() => RequestHandler.DatabaseContext.Configuration.Cluster;

    public override Task<Task> WaitForDatabaseCompletion(Task<Task> onDatabaseCompletionTask, long index, CancellationToken token)
    {
        token.ThrowIfCancellationRequested();

        if (onDatabaseCompletionTask.IsCompletedSuccessfully)
            return onDatabaseCompletionTask;

        // failover
        return Task.FromResult(Task.CompletedTask);
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
