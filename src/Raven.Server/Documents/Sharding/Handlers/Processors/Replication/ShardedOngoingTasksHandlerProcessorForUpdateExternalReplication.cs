using System.Diagnostics.CodeAnalysis;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Replication;
using Raven.Server.Documents.Handlers.Processors.OngoingTasks;
using Raven.Server.ServerWide.Context;
using Sparrow.Json.Parsing;
using Sparrow.Utils;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Replication
{
    internal class ShardedOngoingTasksHandlerProcessorForUpdateExternalReplication : AbstractOngoingTasksHandlerProcessorForUpdateExternalReplication<ShardedDatabaseRequestHandler>
    {
        public ShardedOngoingTasksHandlerProcessorForUpdateExternalReplication([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override string GetDatabaseName() => RequestHandler.DatabaseContext.DatabaseName;

        protected override void FillResponsibleNode(TransactionOperationContext context, DynamicJsonValue responseJson, ExternalReplication watcher)
        {
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Shiran, DevelopmentHelper.Severity.Normal, "get topology for shard");
        }

        protected override ValueTask WaitForIndexNotificationAsync(long index) => RequestHandler.DatabaseContext.Cluster.WaitForExecutionOnAllNodesAsync(index);
    }
}
