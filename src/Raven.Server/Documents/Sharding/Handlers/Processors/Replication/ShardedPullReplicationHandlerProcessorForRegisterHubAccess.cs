using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Exceptions.Sharding;
using Raven.Server.Documents.Handlers.Processors.Replication;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Replication
{
    internal class ShardedPullReplicationHandlerProcessorForRegisterHubAccess : AbstractPullReplicationHandlerProcessorForRegisterHubAccess<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedPullReplicationHandlerProcessorForRegisterHubAccess([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override ValueTask AssertCanExecuteAsync()
        {
            throw new NotSupportedInShardingException("Register Hub Access Command is not supported in sharding");
        }
    }
}
