using JetBrains.Annotations;
using Raven.Client.Http;
using Raven.Server.Documents;
using Raven.Server.Documents.Commands.OngoingTasks;
using Raven.Server.Documents.Handlers.Processors;
using Sparrow.Json;

namespace Raven.Server.Web.System.Processors.OngoingTasks;

internal abstract class AbstractOngoingTasksHandlerProcessorForGetPeriodicBackupTimers<TRequestHandler, TOperationContext> : AbstractHandlerProxyReadProcessor<GetPeriodicBackupTimersCommand.PeriodicBackupTimersResponse, TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    protected AbstractOngoingTasksHandlerProcessorForGetPeriodicBackupTimers([NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override RavenCommand<GetPeriodicBackupTimersCommand.PeriodicBackupTimersResponse> CreateCommandForNode(string nodeTag)
    {
        return new GetPeriodicBackupTimersCommand(nodeTag);
    }
}
