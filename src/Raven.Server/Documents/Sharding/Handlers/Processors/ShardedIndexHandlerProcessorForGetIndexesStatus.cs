using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Indexes;
using Raven.Client.Http;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.Documents.Handlers.Processors.Indexes;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Processors
{
    internal class ShardedIndexHandlerProcessorForGetIndexesStatus : AbstractIndexHandlerProcessorForGetIndexesStatus<ShardedRequestHandler, TransactionOperationContext>
    {
        public ShardedIndexHandlerProcessorForGetIndexesStatus([NotNull] ShardedRequestHandler requestHandler) : base(requestHandler, requestHandler.ContextPool)
        {
        }
        protected override bool SupportsCurrentNode => false;

        protected override ValueTask<IndexingStatus> GetResultForCurrentNodeAsync() => throw new NotSupportedException();

        protected override Task<IndexingStatus> GetResultForRemoteNodeAsync(RavenCommand<IndexingStatus> command) => 
            RequestHandler.ShardExecutor.ExecuteSingleShardAsync(command, GetShardNumber());
    }
}
