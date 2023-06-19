using JetBrains.Annotations;
using Raven.Client.Documents.Operations.Archival;
using Raven.Client.Documents.Operations.Expiration;
using Raven.Client.Exceptions.Sharding;
using Raven.Server.Documents.Handlers.Processors.Archival;
using Raven.Server.Documents.Handlers.Processors.Expiration;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Archival
{
    internal class ShardedArchivalHandlerProcessorForGet : AbstractArchivalHandlerProcessorForGet<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedArchivalHandlerProcessorForGet([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override ArchivalConfiguration GetArchivalConfiguration()
        {
            throw new NotSupportedInShardingException("Data archival for a sharded database is currently not supported");
        }
    }
}
