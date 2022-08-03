using JetBrains.Annotations;
using Raven.Client.Documents.Operations.Replication;
using Raven.Server.Documents.Handlers.Processors.Replication;
using Raven.Server.ServerWide.Context;
using Sparrow.Json.Parsing;
using Sparrow.Utils;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Replication
{
    internal class ShardedPullReplicationHandlerProcessorForUpdatePullReplicationOnSinkNode : AbstractPullReplicationHandlerProcessorForUpdatePullReplicationOnSinkNode<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedPullReplicationHandlerProcessorForUpdatePullReplicationOnSinkNode([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override void FillResponsibleNode(TransactionOperationContext context, DynamicJsonValue responseJson, PullReplicationAsSink pullReplication)
        {
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Shiran, DevelopmentHelper.Severity.Normal, "RavenDB-19069 get topology for shard");
        }
    }
}
