using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents;
using Raven.Server.Documents.Handlers.Processors;
using Sparrow.Json;

namespace Raven.Server.Web.Studio.Processors
{
    internal abstract class AbstractStudioCollectionsHandlerProcessorForRevisionsIds<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
        where TOperationContext : JsonOperationContext
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    {
        protected string Prefix;
        protected int PageSize;

        protected AbstractStudioCollectionsHandlerProcessorForRevisionsIds([NotNull] TRequestHandler requestHandler) : base(requestHandler)
        {
        }

        public override async ValueTask ExecuteAsync()
        {
            Prefix = RequestHandler.GetStringQueryString("prefix", required: true);
            PageSize = RequestHandler.GetPageSize();

            using (ContextPool.AllocateOperationContext(out TOperationContext context))
            using (var token = RequestHandler.CreateHttpRequestBoundOperationToken())
            {
                await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream(), token.Token))
                {
                    writer.WriteStartObject();

                    writer.WritePropertyName("Results");
                    await WriteItemsAsync(context, writer, token.Token);

                    writer.WriteEndObject();
                }
            }
        }

        protected abstract Task WriteItemsAsync(TOperationContext context, AsyncBlittableJsonTextWriter writer, CancellationToken token);
    }
}
