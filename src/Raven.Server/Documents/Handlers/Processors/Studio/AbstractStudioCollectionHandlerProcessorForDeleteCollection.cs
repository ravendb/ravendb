using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Json;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Studio
{
    internal abstract class AbstractStudioCollectionHandlerProcessorForDeleteCollection<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
        where TOperationContext : JsonOperationContext
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    {
        protected AbstractStudioCollectionHandlerProcessorForDeleteCollection([NotNull] TRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected abstract ValueTask DeleteCollectionAsync(TOperationContext context, IDisposable returnContextToPool, string collectionName, HashSet<string> excludeIds, long operationId);

        protected abstract long GetNextOperationId();

        public override async ValueTask ExecuteAsync()
        {
            var returnContextToPool = ContextPool.AllocateOperationContext(out TOperationContext context);

            var collectionName = RequestHandler.GetStringQueryString("name");
            var operationId = RequestHandler.GetLongQueryString("operationId", required: false) ?? GetNextOperationId();
            var excludeIds = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            var reader = await context.ReadForMemoryAsync(RequestHandler.RequestBodyStream(), "ExcludeIds");
            if (reader.TryGet("ExcludeIds", out BlittableJsonReaderArray ids))
            {
                if (ids != null)
                {
                    foreach (LazyStringValue id in ids)
                    {
                        excludeIds.Add(id);
                    }
                }
            }

            await DeleteCollectionAsync(context, returnContextToPool, collectionName, excludeIds, operationId);

            using (ContextPool.AllocateOperationContext(out JsonOperationContext writeContext))
            await using (var writer = new AsyncBlittableJsonTextWriter(writeContext, RequestHandler.ResponseBodyStream()))
            {
                writer.WriteOperationIdAndNodeTag(writeContext, operationId, ServerStore.NodeTag);
            }
        }
    }
}
