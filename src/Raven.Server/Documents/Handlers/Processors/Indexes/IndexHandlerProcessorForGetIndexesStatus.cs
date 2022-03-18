using System.Collections.Generic;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Indexes;
using Raven.Client.Http;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.Indexes
{
    internal class IndexHandlerProcessorForGetIndexesStatus : AbstractIndexHandlerProcessorForGetIndexesStatus<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public IndexHandlerProcessorForGetIndexesStatus([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler, requestHandler.ContextPool)
        {
        }

        protected override bool SupportsCurrentNode => true;

        protected override ValueTask<IndexingStatus> GetResultForCurrentNodeAsync()
        {
            var indexes = new List<IndexingStatus.IndexStatus>();

            foreach (var index in RequestHandler.Database.IndexStore.GetIndexes())
            {
                var indexStatus = new IndexingStatus.IndexStatus
                {
                    Name = index.Name,
                    Status = index.IsPending ? IndexRunningStatus.Pending : index.Status
                };

                indexes.Add(indexStatus);
            }

            var indexesStatus = new IndexingStatus
            {
                Status = RequestHandler.Database.IndexStore.Status,
                Indexes = indexes.ToArray()
            };

            return ValueTask.FromResult(indexesStatus);
        }

        protected override Task<IndexingStatus> GetResultForRemoteNodeAsync(RavenCommand<IndexingStatus> command) => 
            RequestHandler.ExecuteRemoteAsync(command);
    }
}
