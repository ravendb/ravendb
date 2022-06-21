using System;
using System.Net;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client;
using Raven.Client.Http;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.OngoingTasks
{
    internal class OngoingTasksHandlerProcessorForBackupDatabaseNow : AbstractOngoingTasksHandlerProcessorForBackupDatabaseNow<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public OngoingTasksHandlerProcessorForBackupDatabaseNow([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override long GetNextOperationId()
        {
            return RequestHandler.Database.Operations.GetNextOperationId();
        }

        protected override async ValueTask<bool> ScheduleBackupOperationAsync(long taskId, bool isFullBackup, long operationId)
        {
            // task id == raft index
            // we must wait here to ensure that the task was actually created on this node
            await ServerStore.Cluster.WaitForIndexNotification(taskId);

            var nodeTag = RequestHandler.Database.PeriodicBackupRunner.WhoseTaskIsIt(taskId);
            if (nodeTag == null)
                throw new InvalidOperationException($"Couldn't find a node which is responsible for backup task id: {taskId}");

            if (nodeTag == ServerStore.NodeTag)
            {
                RequestHandler.Database.PeriodicBackupRunner.StartBackupTask(taskId, isFullBackup, operationId);
                return true;
            }

            RedirectToRelevantNode(nodeTag);
            return false;
        }

        private void RedirectToRelevantNode(string nodeTag)
        {
            ClusterTopology topology;
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                topology = ServerStore.GetClusterTopology(context);
            }

            var url = topology.GetUrlFromTag(nodeTag);
            if (url == null)
            {
                throw new InvalidOperationException($"Couldn't find the node url for node tag: {nodeTag}");
            }

            var location = url + HttpContext.Request.Path + HttpContext.Request.QueryString;
            HttpContext.Response.StatusCode = (int)HttpStatusCode.TemporaryRedirect;
            HttpContext.Response.Headers.Remove(Constants.Headers.ContentType);
            HttpContext.Response.Headers.Add("Location", location);
        }
    }
}
