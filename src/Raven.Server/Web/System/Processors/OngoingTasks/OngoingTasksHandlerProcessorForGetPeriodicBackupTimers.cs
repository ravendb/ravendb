using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents;
using Raven.Server.Documents.Commands.OngoingTasks;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Http;
using Sparrow.Json;

namespace Raven.Server.Web.System.Processors.OngoingTasks;

internal class OngoingTasksHandlerProcessorForGetPeriodicBackupTimers : AbstractOngoingTasksHandlerProcessorForGetPeriodicBackupTimers<DatabaseRequestHandler, DocumentsOperationContext>
{
    public OngoingTasksHandlerProcessorForGetPeriodicBackupTimers([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override bool SupportsCurrentNode => true;

    protected override async ValueTask HandleCurrentNodeAsync()
    {
        using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
        using (context.OpenReadTransaction())
        await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
        {
            var backups = RequestHandler.Database.PeriodicBackupRunner.GetPeriodicBackupsInformation();
            var result = new GetPeriodicBackupTimersCommand.PeriodicBackupTimersResponse
            {
                Count = backups.Count,
                Timers = backups
            };

            var json = result.ToJson();

            context.Write(writer, json);
        }
    }

    protected override Task HandleRemoteNodeAsync(ProxyCommand<GetPeriodicBackupTimersCommand.PeriodicBackupTimersResponse> command, OperationCancelToken token)
    {
        return RequestHandler.ExecuteRemoteAsync(command, token.Token);
    }
}
