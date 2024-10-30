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

        protected abstract ValueTask<DynamicJsonValue> GetStatsAsync(TOperationContext context);

        public override async ValueTask ExecuteAsync()
        {
            using (ContextPool.AllocateOperationContext(out TOperationContext context))
            {
                DynamicJsonValue result = await GetStatsAsync(context);

                await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
                    context.Write(writer, result);
            }
        }
    }
}
