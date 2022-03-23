using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Http;
using Raven.Server.Documents.Commands;
using Raven.Server.Documents.Commands.Indexes;
using Raven.Server.Documents.Handlers.Processors.Indexes;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Indexes;

internal class ShardedIndexHandlerProcessorForStale : AbstractIndexHandlerProcessorForStale<ShardedDatabaseRequestHandler, TransactionOperationContext>
{
    public ShardedIndexHandlerProcessorForStale([NotNull] ShardedDatabaseRequestHandler requestHandler)
        : base(requestHandler, requestHandler.ContextPool)
    {
    }

    protected override bool SupportsCurrentNode => false;

    protected override ValueTask<GetIndexStalenessCommand.IndexStaleness> GetResultForCurrentNodeAsync() => throw new NotSupportedException();

    protected override Task<GetIndexStalenessCommand.IndexStaleness> GetResultForRemoteNodeAsync(RavenCommand<GetIndexStalenessCommand.IndexStaleness> command)
    {
        var shardNumber = GetShardNumber();

        return RequestHandler.ShardExecutor.ExecuteSingleShardAsync(command, shardNumber);
    }
}
