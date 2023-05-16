using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Sharding.Handlers;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Http;
using Raven.Server.Web.Studio.Processors;

namespace Raven.Server.Web.Studio.Sharding.Processors;

internal class ShardedStudioDatabaseTasksHandlerProcessorForRestartDatabase : AbstractStudioDatabaseTasksHandlerProcessorForRestartDatabase<ShardedDatabaseRequestHandler, TransactionOperationContext>
{
    public ShardedStudioDatabaseTasksHandlerProcessorForRestartDatabase([NotNull] ShardedDatabaseRequestHandler requestHandler)
        : base(requestHandler)
    {
    }

    protected override bool SupportsCurrentNode => false;

    protected override ValueTask HandleCurrentNodeAsync() => throw new NotSupportedException();

    protected override Task HandleRemoteNodeAsync(ProxyCommand<object> command, OperationCancelToken token)
    {
        var shardNumber = GetShardNumber();
        return RequestHandler.DatabaseContext.ShardExecutor.ExecuteSingleShardAsync(command, shardNumber, token.Token);
    }
}
