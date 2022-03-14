using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Indexes;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors
{
    internal abstract class AbstractIndexHandlerProcessorForGetIndexesStatus<TRequestHandler, TOperationContext> : AbstractHandlerProcessor<TRequestHandler, TOperationContext>
        where TRequestHandler : RequestHandler
        where TOperationContext : JsonOperationContext
    {
        protected AbstractIndexHandlerProcessorForGetIndexesStatus([NotNull] TRequestHandler requestHandler,
            [NotNull] JsonContextPoolBase<TOperationContext> operationContext) : base(requestHandler, operationContext)
        {
        }

        protected abstract ValueTask<IndexingStatus> GetIndexesStatusAsync();

        public override async ValueTask ExecuteAsync()
        {
            var indexesStatus = await GetIndexesStatusAsync();

            using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
            {
                writer.WriteStartObject();

                writer.WritePropertyName(nameof(IndexingStatus.Status));
                writer.WriteString(indexesStatus.Status.ToString());
                writer.WriteComma();

                writer.WritePropertyName(nameof(IndexingStatus.Indexes));
                writer.WriteStartArray();
                var isFirst = true;
                foreach (var index in indexesStatus.Indexes)
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
