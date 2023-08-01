using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Sharding.Handlers;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Http;
using Raven.Server.Web.Studio.Processors;

namespace Raven.Server.Web.Studio.Sharding.Processors;

internal sealed class ShardedStudioDatabaseTasksHandlerProcessorForRestartDatabase : AbstractStudioDatabaseTasksHandlerProcessorForRestartDatabase<ShardedDatabaseRequestHandler, TransactionOperationContext>
{
    public ShardedStudioDatabaseTasksHandlerProcessorForRestartDatabase([NotNull] ShardedDatabaseRequestHandler requestHandler)
        : base(requestHandler)
    {
    }

    protected override bool SupportsOptionalShardNumber => true;

    protected override Task HandleRemoteNodeAsync(ProxyCommand<object> command, OperationCancelToken token)
    {
        return TryGetShardNumber(out int shardNumber) 
            ? RequestHandler.DatabaseContext.ShardExecutor.ExecuteSingleShardAsync(command, shardNumber, token.Token) 
            : RequestHandler.DatabaseContext.AllOrchestratorNodesExecutor.ExecuteForNodeAsync(command, command.SelectedNodeTag, token.Token);
    }
}
