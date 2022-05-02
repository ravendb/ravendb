using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Documents.Operations.Replication;
using Raven.Server.Documents.Replication;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers.Processors.OngoingTasks
{
    internal class OngoingTasksHandlerProcessorForUpdateExternalReplication : AbstractOngoingTasksHandlerProcessorForUpdateExternalReplication<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public OngoingTasksHandlerProcessorForUpdateExternalReplication([NotNull] DatabaseRequestHandler requestHandler) 
            : base(requestHandler)
        {
        }

        protected override void FillResponsibleNode(TransactionOperationContext context, DynamicJsonValue responseJson, ExternalReplication watcher)
        {
            var databaseName = RequestHandler.DatabaseName;

            using (context.OpenReadTransaction())
            {
                var topology = RequestHandler.ServerStore.Cluster.ReadDatabaseTopology(context, databaseName);
                var taskStatus = ReplicationLoader.GetExternalReplicationState(RequestHandler.ServerStore, databaseName, watcher.TaskId);
                responseJson[nameof(OngoingTask.ResponsibleNode)] = RequestHandler.ServerStore.WhoseTaskIsIt(topology, watcher, taskStatus);
            }
        }
    }
}
