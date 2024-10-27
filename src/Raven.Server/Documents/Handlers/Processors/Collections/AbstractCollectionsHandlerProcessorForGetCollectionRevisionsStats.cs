using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers.Processors.Collections
{
    internal abstract class AbstractCollectionsHandlerProcessorForGetCollectionRevisionsStats<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
        where TOperationContext : JsonOperationContext
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    {
        protected AbstractCollectionsHandlerProcessorForGetCollectionRevisionsStats([NotNull] TRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected abstract ValueTask<DynamicJsonValue> GetStatsAsync(TOperationContext context, CancellationToken tokenToken);

        public override async ValueTask ExecuteAsync()
        {
            using (var token = RequestHandler.CreateHttpRequestBoundOperationToken())
            using (ContextPool.AllocateOperationContext(out TOperationContext context))
            {
                DynamicJsonValue result = await GetStatsAsync(context, token.Token);

                await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream(), token.Token))
                    context.Write(writer, result);
            }
        }
    }
}
