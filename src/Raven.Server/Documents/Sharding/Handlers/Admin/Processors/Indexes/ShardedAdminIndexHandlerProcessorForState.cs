using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Indexes;
using Raven.Client.Http;
using Raven.Server.Documents.Handlers.Admin.Processors.Indexes;
using Raven.Server.Documents.Indexes;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Admin.Processors.Indexes;

internal class ShardedAdminIndexHandlerProcessorForState : AbstractAdminIndexHandlerProcessorForState<ShardedDatabaseRequestHandler, TransactionOperationContext>
{
    public ShardedAdminIndexHandlerProcessorForState(IndexState state, [NotNull] ShardedDatabaseRequestHandler requestHandler) 
        : base(state, requestHandler, requestHandler.ContextPool)
    {
    }

    protected override bool SupportsCurrentNode => false;

    protected override ValueTask ExecuteForCurrentNodeAsync() => throw new NotSupportedException();

    protected override Task ExecuteForRemoteNodeAsync(RavenCommand command)
    {
        var shardNumber = GetShardNumber();

        return RequestHandler.ShardExecutor.ExecuteSingleShardAsync(command, shardNumber);
    }

    protected override AbstractIndexStateController GetIndexStateProcessor()
    {
        return RequestHandler.DatabaseContext.Indexes.State;
    }
}
