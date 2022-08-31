using System.Collections.Generic;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Exceptions.Sharding;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Server.Documents.Handlers.Processors.OngoingTasks;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.OngoingTasks
{
    internal class ShardedOngoingTasksHandlerProcessorForGetPullReplicationHubTasksInfo : AbstractOngoingTasksHandlerProcessorForGetPullReplicationHubTasksInfo<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedOngoingTasksHandlerProcessorForGetPullReplicationHubTasksInfo([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override void AssertCanExecute()
        {
            throw new NotSupportedInShardingException("Get Pull Replication Hub Task Info is not supported in sharding");
        }

        protected override IEnumerable<OngoingTaskPullReplicationAsHub> GetOngoingTasks(TransactionOperationContext context, DatabaseRecord databaseRecord, ClusterTopology clusterTopology, long key)
        {
            throw new NotSupportedInShardingException("Get Pull Replication Hub Task Info is not supported in sharding");
        }
    }
}
