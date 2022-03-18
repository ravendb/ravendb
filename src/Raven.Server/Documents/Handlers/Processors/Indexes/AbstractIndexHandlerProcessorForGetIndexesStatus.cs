using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Http;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Indexes
{
    internal abstract class
        AbstractIndexHandlerProcessorForGetIndexesStatus<TRequestHandler, TOperationContext> : AbstractHandlerReadProcessor<IndexingStatus, TRequestHandler,
            TOperationContext>
        where TRequestHandler : RequestHandler
        where TOperationContext : JsonOperationContext
    {
        protected AbstractIndexHandlerProcessorForGetIndexesStatus([NotNull] TRequestHandler requestHandler,
            [NotNull] JsonContextPoolBase<TOperationContext> operationContext) : base(requestHandler, operationContext)
        {
        }

        protected override RavenCommand<IndexingStatus> CreateCommandForNode(string nodeTag) =>
            new GetIndexingStatusOperation.GetIndexingStatusCommand(nodeTag);

        protected override async ValueTask WriteResultAsync(IndexingStatus result)
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
