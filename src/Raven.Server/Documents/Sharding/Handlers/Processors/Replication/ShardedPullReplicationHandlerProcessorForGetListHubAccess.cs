using JetBrains.Annotations;
using Raven.Client.Exceptions.Sharding;
using Raven.Server.Documents.Handlers.Processors.Replication;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Replication
{
    internal class ShardedPullReplicationHandlerProcessorForGetListHubAccess : AbstractPullReplicationHandlerProcessorForGetListHubAccess<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedPullReplicationHandlerProcessorForGetListHubAccess([NotNull] ShardedDatabaseRequestHandler requestHandler)
            : base(requestHandler, requestHandler.ContextPool)
        {
        }

        protected override void AssertCanExecute()
        {
            throw new NotSupportedInShardingException("Get List of Hub Access Command is not supported in sharding");
        }

        protected override string GetDatabaseName() => RequestHandler.DatabaseContext.DatabaseName;
    }
}
