using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations;
using Raven.Server.Documents;
using Raven.Server.Documents.Operations;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Studio.Processors;
using Sparrow.Json;

namespace Raven.Server.Web.Studio
{
    public class StudioCollectionsHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/studio/collections/preview", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task PreviewCollection()
        {
            using (var processor = new StudioCollectionsHandlerProcessorForPreviewCollection(this, Database))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/studio/collections/docs", "DELETE", AuthorizationStatus.ValidUser, EndpointType.Write)]
        public async Task Delete()
        {
            var returnContextToPool = ContextPool.AllocateOperationContext(out DocumentsOperationContext context);

            var excludeIds = new HashSet<string>();

            var reader = await context.ReadForMemoryAsync(RequestBodyStream(), "ExcludeIds");
            if (reader.TryGet("ExcludeIds", out BlittableJsonReaderArray ids))
            {
                foreach (LazyStringValue id in ids)
                {
                    excludeIds.Add(id);
                }
            }

            await ExecuteCollectionOperation((runner, collectionName, options, onProgress, token) => Task.Run(async () => await runner.ExecuteDelete(collectionName, 0, long.MaxValue, options, onProgress, token)),
                context, returnContextToPool, OperationType.DeleteByCollection, excludeIds);
        }

        private async Task ExecuteCollectionOperation(Func<CollectionRunner, string, CollectionOperationOptions, Action<IOperationProgress>, OperationCancelToken, Task<IOperationResult>> operation, DocumentsOperationContext docsContext, IDisposable returnContextToPool, OperationType operationType, HashSet<string> excludeIds)
        {
            var collectionName = GetStringQueryString("name");

            var token = CreateTimeLimitedCollectionOperationToken();

            var collectionRunner = new StudioCollectionRunner(Database, docsContext, excludeIds);

            var operationId = Database.Operations.GetNextOperationId();

            // use default options
            var options = new CollectionOperationOptions();

            var task = Database.Operations.AddOperation(Database.Name, collectionName, operationType, onProgress =>
                     operation(collectionRunner, collectionName, options, onProgress, token), operationId, token: token);

            _ = task.ContinueWith(_ => returnContextToPool.Dispose());

            using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteOperationIdAndNodeTag(context, operationId, ServerStore.NodeTag);
            }
        }
    }
}
