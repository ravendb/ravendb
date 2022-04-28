using System.Collections.Generic;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Indexes;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Http;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Indexes
{
    internal class IndexHandlerProcessorForGetIndexesStatus : AbstractIndexHandlerProcessorForGetIndexesStatus<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public IndexHandlerProcessorForGetIndexesStatus([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override bool SupportsCurrentNode => true;

        protected override ValueTask HandleCurrentNodeAsync()
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

            return WriteResultAsync(indexesStatus);
        }

        protected override Task HandleRemoteNodeAsync(ProxyCommand<IndexingStatus> command) => RequestHandler.ExecuteRemoteAsync(command);

        private async ValueTask WriteResultAsync(IndexingStatus result)
        {
            using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
            {
                writer.WriteStartObject();

                writer.WritePropertyName(nameof(IndexingStatus.Status));
                writer.WriteString(result.Status.ToString());
                writer.WriteComma();

                writer.WritePropertyName(nameof(IndexingStatus.Indexes));
                writer.WriteStartArray();
                var isFirst = true;
                foreach (var index in result.Indexes)
                {
                    if (isFirst == false)
                        writer.WriteComma();

                    isFirst = false;

                    writer.WriteStartObject();

                    writer.WritePropertyName(nameof(IndexingStatus.IndexStatus.Name));
                    writer.WriteString(index.Name);

                    writer.WriteComma();

                    writer.WritePropertyName(nameof(IndexingStatus.IndexStatus.Status));
                    writer.WriteString(index.Status.ToString());

                    writer.WriteEndObject();
                }

                writer.WriteEndArray();

                writer.WriteEndObject();
            }
        }
    }
}
