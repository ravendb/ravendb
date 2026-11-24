using JetBrains.Annotations;
using Raven.Client.Http;
using Raven.Server.Documents;
using Raven.Server.Documents.Commands.OngoingTasks;
using Raven.Server.Documents.Handlers.Processors;
using Sparrow.Json;

namespace Raven.Server.Web.System.Processors.OngoingTasks;

internal abstract class AbstractOngoingTasksHandlerProcessorForGetPeriodicBackupSchedule<TRequestHandler, TOperationContext> : AbstractHandlerProxyReadProcessor<GetPeriodicBackupScheduleCommand.PeriodicBackupScheduleResponse, TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    protected AbstractOngoingTasksHandlerProcessorForGetPeriodicBackupSchedule([NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override RavenCommand<GetPeriodicBackupScheduleCommand.PeriodicBackupScheduleResponse> CreateCommandForNode(string nodeTag)
    {
        return new GetPeriodicBackupScheduleCommand(nodeTag);
    }
}
