using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Commands.Indexes;
using Raven.Server.Documents.Sharding.Handlers;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Http;
using Raven.Server.Web.Studio.Processors;

namespace Raven.Server.Web.Studio.Sharding.Processors;

internal class ShardedStudioIndexHandlerProcessorForGetIndexErrorsCount : AbstractStudioIndexHandlerProcessorForGetIndexErrorsCount<ShardedDatabaseRequestHandler, TransactionOperationContext>
{
    public ShardedStudioIndexHandlerProcessorForGetIndexErrorsCount([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override bool SupportsCurrentNode => false;

    protected override ValueTask HandleCurrentNodeAsync() => throw new NotSupportedException();

    protected override Task HandleRemoteNodeAsync(ProxyCommand<GetIndexErrorsCountCommand.IndexErrorsCount[]> command)
    {
        var shardNumber = GetShardNumber();

        return RequestHandler.ShardExecutor.ExecuteSingleShardAsync(command, shardNumber);
    }
}
