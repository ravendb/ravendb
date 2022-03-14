using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Http;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.Documents.Sharding.Operations;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Processors
{
    internal class ShardedIndexHandlerProcessorForGetIndexesStatus : AbstractIndexHandlerProcessorForGetIndexesStatus<ShardedRequestHandler, TransactionOperationContext>
    {
        public ShardedIndexHandlerProcessorForGetIndexesStatus([NotNull] ShardedRequestHandler requestHandler) : base(requestHandler, requestHandler.ContextPool)
        {
        }

        protected override async ValueTask<IndexingStatus> GetIndexesStatusAsync()
        {
            var op = new ShardedIndexesStatusOperation();
            return await RequestHandler.ShardExecutor.ExecuteParallelForAllAsync(op);
        }

        internal readonly struct ShardedIndexesStatusOperation : IShardedOperation<IndexingStatus>
        {
            public IndexingStatus Combine(Memory<IndexingStatus> results)
            {
                if (results.Length == 0)
                    return null;

                var span = results.Span;

                var combined = new IndexingStatus
                {
                    Status = IndexRunningStatus.Running, // default value
                    Indexes = span[0].Indexes
                };

                for (var i = 0; i < span[0].Indexes.Length; i++)
                    combined.Indexes[i].Status = IndexRunningStatus.Running;

                return combined;
            }

            public RavenCommand<IndexingStatus> CreateCommandForShard(int shard) => new GetIndexingStatusOperation.GetIndexingStatusCommand();

        }
    }
}
