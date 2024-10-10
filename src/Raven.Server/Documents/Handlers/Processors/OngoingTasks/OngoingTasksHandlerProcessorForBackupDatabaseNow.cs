using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Server.Web.Http;

namespace Raven.Server.Documents.Handlers.Processors.OngoingTasks
{
    internal sealed class OngoingTasksHandlerProcessorForBackupDatabaseNow : AbstractOngoingTasksHandlerProcessorForBackupDatabaseNow<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public OngoingTasksHandlerProcessorForBackupDatabaseNow([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override long GetNextOperationId()
        {
            return RequestHandler.Database.Operations.GetNextOperationId();
        }
        
        protected override async ValueTask<(long OperationId, bool IsResponsibleNode)> ScheduleBackupOperationAsync(long taskId, bool isFullBackup, long operationId, DateTime? startTime)
        {
            var nodeTag = await BackupUtils.WaitAndGetResponsibleNodeAsync(taskId, RequestHandler.Database);

            if (nodeTag == ServerStore.NodeTag)
            {
                operationId = RequestHandler.Database.PeriodicBackupRunner.StartBackupTask(taskId, isFullBackup, operationId, startTime);
                return (operationId, IsResponsibleNode: true);
            }

            //redirect
            var cmd = new StartBackupOperation.StartBackupCommand(isFullBackup, taskId, operationId, startTime);
            cmd.SelectedNodeTag = nodeTag;
            await RequestHandler.ExecuteRemoteAsync(new ProxyCommand<OperationIdResult<StartBackupOperationResult>>(cmd, HttpContext.Response));
            
            return (operationId, IsResponsibleNode: false);
        }
    }
}
