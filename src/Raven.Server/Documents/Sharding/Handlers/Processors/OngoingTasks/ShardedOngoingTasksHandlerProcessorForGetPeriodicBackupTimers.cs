using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Commands.OngoingTasks;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Http;
using Raven.Server.Web.System.Processors.OngoingTasks;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.OngoingTasks;

internal class ShardedOngoingTasksHandlerProcessorForGetPeriodicBackupTimers : AbstractOngoingTasksHandlerProcessorForGetPeriodicBackupTimers<ShardedDatabaseRequestHandler, TransactionOperationContext>
{
    public ShardedOngoingTasksHandlerProcessorForGetPeriodicBackupTimers([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override bool SupportsCurrentNode => false;

    protected override ValueTask HandleCurrentNodeAsync() => throw new NotSupportedException();

    protected override Task HandleRemoteNodeAsync(ProxyCommand<GetPeriodicBackupTimersCommand.PeriodicBackupTimersResponse> command, OperationCancelToken token)
    {
        var shardNumber = GetShardNumber();

        return RequestHandler.ShardExecutor.ExecuteSingleShardAsync(command, shardNumber, token.Token);
    }
}
