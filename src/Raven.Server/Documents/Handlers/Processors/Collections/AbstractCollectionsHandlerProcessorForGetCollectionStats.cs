using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers.Processors.Collections
{
    internal abstract class AbstractCollectionsHandlerProcessorForGetCollectionStats<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
        where TOperationContext : JsonOperationContext
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    {
        private readonly bool _detailed;

        public AbstractCollectionsHandlerProcessorForGetCollectionStats([NotNull] TRequestHandler requestHandler, bool detailed) : base(requestHandler)
        {
            _detailed = detailed;
        }

        protected abstract ValueTask<DynamicJsonValue> GetStatsAsync(TOperationContext context, bool detailed);

        public override async ValueTask ExecuteAsync()
        {
            using (ContextPool.AllocateOperationContext(out TOperationContext context))
            {
                DynamicJsonValue result = await GetStatsAsync(context, detailed: _detailed);

                await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
                    context.Write(writer, result);
            }
        }
    }
}
