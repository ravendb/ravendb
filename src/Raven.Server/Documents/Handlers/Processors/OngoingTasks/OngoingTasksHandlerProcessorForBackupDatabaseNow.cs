using System;
using System.Net;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client;
using Raven.Client.Http;
using Raven.Server.ServerWide.Context;

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
        
        protected override async ValueTask<(long, bool)> ScheduleBackupOperationAsync(long taskId, bool isFullBackup, long operationId, bool inProgressInAnotherShard, DateTime? startTime)
        {
            // task id == raft index
            // we must wait here to ensure that the task was actually created on this node
            await ServerStore.Cluster.WaitForIndexNotification(taskId);

            var nodeTag = RequestHandler.Database.PeriodicBackupRunner.WhoseTaskIsIt(taskId);
            if (nodeTag == null)
            {
                // this can happen if the database was just created or if a new task that was just created
                // we'll wait for the cluster observer to give more time for the database stats to become stable,
                // and then we'll wait for the cluster observer to determine the responsible node for the backup

                var task = Task.Delay(RequestHandler.Database.Configuration.Cluster.StabilizationTime.AsTimeSpan + RequestHandler.Database.Configuration.Cluster.StabilizationTime.AsTimeSpan);

                while (true)
                {
                    if (Task.WaitAny(new[] { task }, millisecondsTimeout: 100) == 0)
                    {
                        throw new InvalidOperationException($"Couldn't find a node which is responsible for backup task id: {taskId}");
                    }

                    nodeTag = RequestHandler.Database.PeriodicBackupRunner.WhoseTaskIsIt(taskId);
                    if (nodeTag != null)
                        break;
                }
            }

            if (nodeTag == ServerStore.NodeTag)
            {
                // if in progress in a different shard, don't call the backup. but we must return the operationId so WaitForCompletion will know this shard is done
                if (inProgressInAnotherShard)
                    return (operationId, true);

                operationId = RequestHandler.Database.PeriodicBackupRunner.StartBackupTask(taskId, isFullBackup, operationId, startTime);
                return (operationId, true);
            }

            RedirectToRelevantNode(nodeTag);
            return (operationId, false);
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
            HttpContext.Response.Headers["Location"] = location;
        }
    }
}
