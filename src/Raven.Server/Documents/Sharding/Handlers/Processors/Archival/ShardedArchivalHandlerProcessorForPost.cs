using JetBrains.Annotations;
using Raven.Client.Exceptions.Sharding;
using Raven.Server.Documents.Handlers.Processors.Archival;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Archival
{
    internal class ShardedArchivalHandlerProcessorForPost : AbstractArchivalHandlerProcessorForPost<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedArchivalHandlerProcessorForPost([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
        {
            throw new NotSupportedInShardingException("Data archival for a sharded database is currently not supported");
        }
    }
}
