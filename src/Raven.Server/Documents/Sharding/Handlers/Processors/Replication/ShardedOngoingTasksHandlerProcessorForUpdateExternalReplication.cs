using System;
using System.Diagnostics.CodeAnalysis;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Documents.Operations.Replication;
using Raven.Server.Documents.Handlers.Processors.OngoingTasks;
using Raven.Server.Documents.Replication;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
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
            var topology = RequestHandler.DatabaseContext.DatabaseRecord.Sharding.Orchestrator?.Topology;
            if (topology == null)
                throw new InvalidOperationException($"The database record '{RequestHandler.DatabaseName}' doesn't contain topology.");

            var taskStatus = ReplicationLoader.GetExternalReplicationState(RequestHandler.ServerStore, RequestHandler.DatabaseName, watcher.TaskId);
            responseJson[nameof(OngoingTask.ResponsibleNode)] = RequestHandler.ServerStore.WhoseTaskIsIt(topology, watcher, taskStatus);
        }

        protected override void OnBeforeUpdateConfiguration(ref BlittableJsonReaderObject configuration, JsonOperationContext context)
        {
            if (configuration.TryGet(nameof(UpdateExternalReplicationCommand.Watcher), out BlittableJsonReaderObject watcher) &&
                watcher.TryGet(nameof(ExternalReplication.MentorNode), out string mentor) &&
                string.IsNullOrEmpty(mentor) == false)
                throw new InvalidOperationException($"Can't create or update external replication. Choosing a mentor node for an ongoing task is not supported in sharding.");
        }
    }
}
