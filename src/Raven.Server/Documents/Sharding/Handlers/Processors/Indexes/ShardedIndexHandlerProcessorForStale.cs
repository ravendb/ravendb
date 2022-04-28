using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Commands.Indexes;
using Raven.Server.Documents.Handlers.Processors.Indexes;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Http;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Indexes;

internal class ShardedIndexHandlerProcessorForStale : AbstractIndexHandlerProcessorForStale<ShardedDatabaseRequestHandler, TransactionOperationContext>
{
    public ShardedIndexHandlerProcessorForStale([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override bool SupportsCurrentNode => false;

    protected override ValueTask HandleCurrentNodeAsync() => throw new NotSupportedException();

    protected override Task HandleRemoteNodeAsync(ProxyCommand<GetIndexStalenessCommand.IndexStaleness> command)
    {
        var shardNumber = GetShardNumber();

        return RequestHandler.ShardExecutor.ExecuteSingleShardAsync(command, shardNumber);
    }
}
