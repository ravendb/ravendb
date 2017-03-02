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
using Sparrow.Json;

namespace Raven.Server.Web.Studio
{
    public class StudioCollectionsHandler : DatabaseRequestHandler
    {

        [RavenAction("/databases/*/studio/collections/docs", "DELETE")]
        public Task Delete()
        {
            DocumentsOperationContext context;
            var returnContextToPool = ContextPool.AllocateOperationContext(out context);

            var excludeIds = new HashSet<LazyStringValue>();

            var reader = context.Read(RequestBodyStream(), "ExcludeIds");
            BlittableJsonReaderArray idsBlittable;
            if (reader.TryGet("ExcludeIds", out idsBlittable)) 
            {
                foreach (LazyStringValue item in idsBlittable)
                {
                    excludeIds.Add(item);
                }
            }

            ExecuteCollectionOperation((runner, collectionName, options, onProgress, token) => Task.Run(() => runner.ExecuteDelete(collectionName, options, context, onProgress, token)),
                context, returnContextToPool, DatabaseOperations.OperationType.DeleteByCollection, excludeIds);
            return Task.CompletedTask;
        }

       
        private void ExecuteCollectionOperation(Func<CollectionRunner, string, CollectionOperationOptions, Action<IOperationProgress>, OperationCancelToken, Task<IOperationResult>> operation, DocumentsOperationContext context, IDisposable returnContextToPool, DatabaseOperations.OperationType operationType, HashSet<LazyStringValue> excludeIds)
        {
            var collectionName = GetStringQueryString("name");

            var token = CreateTimeLimitedOperationToken();

            var collectionRunner = new StudioCollectionRunner(Database, context, excludeIds);

            var operationId = Database.Operations.GetNextOperationId();

            // use default options
            var options = new CollectionOperationOptions();

            var task = Database.Operations.AddOperation(collectionName, operationType, onProgress =>
                    operation(collectionRunner, collectionName, options, onProgress, token), operationId, token);

            task.ContinueWith(_ => returnContextToPool.Dispose());

            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteOperationId(context, operationId);
            }
        }
    }
}