using JetBrains.Annotations;
using Raven.Client.Exceptions.Sharding;
using Raven.Server.Documents.Handlers.Processors.Replication;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Replication
{
    internal class ShardedPullReplicationHandlerProcessorForGenerateCertificate : AbstractPullReplicationHandlerProcessorForGenerateCertificate<ShardedDatabaseRequestHandler>
    {
        public ShardedPullReplicationHandlerProcessorForGenerateCertificate([NotNull] ShardedDatabaseRequestHandler requestHandler)
            : base(requestHandler)
        {
        }

        protected override void AssertCanExecute()
        {
            throw new NotSupportedInShardingException("Generate Certificate for Pull Replication is not supported in sharding");
        }
    }
}
