using System;
using System.Diagnostics.CodeAnalysis;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Documents.Operations.Replication;
using Raven.Server.Documents.Handlers.Processors.OngoingTasks;
using Raven.Server.Documents.Replication;
using Raven.Server.ServerWide.Context;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Replication
{
    internal class ShardedOngoingTasksHandlerProcessorForUpdateExternalReplication : AbstractOngoingTasksHandlerProcessorForUpdateExternalReplication<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedOngoingTasksHandlerProcessorForUpdateExternalReplication([NotNull] ShardedDatabaseRequestHandler requestHandler) 
            : base(requestHandler)
        {
        }

        protected override void FillResponsibleNode(TransactionOperationContext context, DynamicJsonValue responseJson, ExternalReplication watcher)
        {
            var databaseName = RequestHandler.DatabaseName;

            using (context.OpenReadTransaction())
            {
                var record = RequestHandler.ServerStore.Cluster.ReadRawDatabaseRecord(context, databaseName);
                var topology = record.Sharding.Orchestrator?.Topology;

                if (topology == null)
                    throw new InvalidOperationException($"The database record '{databaseName}' doesn't contain topology.");

                var taskStatus = ReplicationLoader.GetExternalReplicationState(RequestHandler.ServerStore, databaseName, watcher.TaskId);
                responseJson[nameof(OngoingTask.ResponsibleNode)] = RequestHandler.ServerStore.WhoseTaskIsIt(topology, watcher, taskStatus);
            }
        }
    }
}
