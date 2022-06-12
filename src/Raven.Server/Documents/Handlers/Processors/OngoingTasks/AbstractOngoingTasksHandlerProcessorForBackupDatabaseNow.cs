using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.Backups;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.OngoingTasks
{
    internal abstract class AbstractOngoingTasksHandlerProcessorForBackupDatabaseNow<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
        where TOperationContext : JsonOperationContext
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    {
        protected AbstractOngoingTasksHandlerProcessorForBackupDatabaseNow([NotNull] TRequestHandler requestHandler) : base(requestHandler)
        {
        }
        protected abstract ValueTask<bool> ScheduleBackupOperation(long taskId, bool isFullBackup, long operationId);

        protected abstract long GetNextOperationId();

        public override async ValueTask ExecuteAsync()
        {
            var taskId = RequestHandler.GetLongQueryString("taskId");
            var isFullBackup = RequestHandler.GetBoolValueQueryString("isFullBackup", required: false) ?? true;
            var operationId = RequestHandler.GetLongQueryString("operationId", required: false) ?? GetNextOperationId();

            var isResponsibleNode = await ScheduleBackupOperation(taskId, isFullBackup, operationId);

            if (isResponsibleNode)
            {
                using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
                await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
                {
                    writer.WriteStartObject();
                    writer.WritePropertyName(nameof(StartBackupOperationResult.ResponsibleNode));
                    writer.WriteString(ServerStore.NodeTag);
                    writer.WriteComma();
                    writer.WritePropertyName(nameof(StartBackupOperationResult.OperationId));
                    writer.WriteInteger(operationId);
                    writer.WriteEndObject();
                }
            }
        }
    }
}
