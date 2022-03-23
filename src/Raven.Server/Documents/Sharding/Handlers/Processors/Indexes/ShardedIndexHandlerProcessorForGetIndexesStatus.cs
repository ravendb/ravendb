using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Indexes;
using Raven.Client.Http;
using Raven.Server.Documents.Handlers.Processors.Indexes;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Indexes
{
    internal class ShardedIndexHandlerProcessorForGetIndexesStatus : AbstractIndexHandlerProcessorForGetIndexesStatus<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedIndexHandlerProcessorForGetIndexesStatus([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler, requestHandler.ContextPool)
        {
        }
        protected override bool SupportsCurrentNode => false;

        protected override ValueTask<IndexingStatus> GetResultForCurrentNodeAsync() => throw new NotSupportedException();

        protected override Task<IndexingStatus> GetResultForRemoteNodeAsync(RavenCommand<IndexingStatus> command) => 
            RequestHandler.ShardExecutor.ExecuteSingleShardAsync(command, GetShardNumber());
    }
}
