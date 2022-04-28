using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.Replication;
using Raven.Server.Documents.Handlers.Processors.Replication;
using Raven.Server.ServerWide.Context;
using Sparrow.Json.Parsing;
using Sparrow.Utils;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Replication
{
    internal class ShardedPullReplicationHandlerProcessorForUpdatePullReplicationOnSinkNode : AbstractPullReplicationHandlerProcessorForUpdatePullReplicationOnSinkNode<ShardedDatabaseRequestHandler>
    {
        public ShardedPullReplicationHandlerProcessorForUpdatePullReplicationOnSinkNode([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override string GetDatabaseName() => RequestHandler.DatabaseContext.DatabaseName;

        protected override void FillResponsibleNode(TransactionOperationContext context, DynamicJsonValue responseJson, PullReplicationAsSink pullReplication)
        {
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Shiran, DevelopmentHelper.Severity.Normal, "get topology for shard");
        }

        protected override ValueTask WaitForIndexNotificationAsync(long index) => RequestHandler.DatabaseContext.Cluster.WaitForExecutionOnAllNodesAsync(index);
    }
}
