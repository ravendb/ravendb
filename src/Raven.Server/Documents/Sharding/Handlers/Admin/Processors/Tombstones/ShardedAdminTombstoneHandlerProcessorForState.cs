using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Commands.Tombstones;
using Raven.Server.Documents.Handlers.Admin.Processors.Tombstones;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Http;

namespace Raven.Server.Documents.Sharding.Handlers.Admin.Processors.Tombstones;

internal sealed class ShardedAdminTombstoneHandlerProcessorForState : AbstractAdminTombstoneHandlerProcessorForState<ShardedDatabaseRequestHandler, TransactionOperationContext>
{
    public ShardedAdminTombstoneHandlerProcessorForState([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override bool SupportsCurrentNode => false;

    protected override ValueTask HandleCurrentNodeAsync() => throw new NotSupportedException();

    protected override Task HandleRemoteNodeAsync(ProxyCommand<GetTombstonesStateCommand.Response> command, OperationCancelToken token)
    {
        var shardNumber = GetShardNumber();

        return RequestHandler.DatabaseContext.ShardExecutor.ExecuteSingleShardAsync(command, shardNumber, token.Token);
    }
}
