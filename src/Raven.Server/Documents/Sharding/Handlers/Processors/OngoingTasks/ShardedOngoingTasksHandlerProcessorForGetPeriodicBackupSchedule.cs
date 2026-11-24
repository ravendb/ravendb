using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Commands.OngoingTasks;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Http;
using Raven.Server.Web.System.Processors.OngoingTasks;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.OngoingTasks;

internal sealed class ShardedOngoingTasksHandlerProcessorForGetPeriodicBackupSchedule : AbstractOngoingTasksHandlerProcessorForGetPeriodicBackupSchedule<ShardedDatabaseRequestHandler, TransactionOperationContext>
{
    public ShardedOngoingTasksHandlerProcessorForGetPeriodicBackupSchedule([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override bool SupportsCurrentNode => false;

    protected override ValueTask HandleCurrentNodeAsync() => throw new NotSupportedException();

    protected override Task HandleRemoteNodeAsync(ProxyCommand<GetPeriodicBackupScheduleCommand.PeriodicBackupScheduleResponse> command, OperationCancelToken token)
    {
        var shardNumber = GetShardNumber();

        return RequestHandler.ShardExecutor.ExecuteSingleShardAsync(command, shardNumber, token.Token);
    }
}
