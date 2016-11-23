using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client.Data;
using Raven.Client.Data.Queries;
using Raven.Server.Documents.Queries;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Raven.Client.Data.Collection;

namespace Raven.Server.Documents.Handlers
{
    public class CollectionsHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/collections/stats", "GET", "/databases/{databaseName:string}/collections/stats")]
        public Task GetCollectionStats()
        {
            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            {
                context.OpenReadTransaction();
                var collections = new DynamicJsonValue();
                var result = new DynamicJsonValue
                {
                    ["NumberOfDocuments"] = Database.DocumentsStorage.GetNumberOfDocuments(context),
                    ["Collections"] = collections
                };

                foreach (var collectionStat in Database.DocumentsStorage.GetCollections(context))
                {
                    collections[collectionStat.Name] = collectionStat.Count;
                }
                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                    context.Write(writer, result);
            }
            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/collections/docs", "GET", "/databases/{databaseName:string}/collections/docs?name={collectionName:string}&start={pageStart:int|optional}&pageSize={pageSize:int|optional(25)}")]
        public Task GetCollectionDocuments()
        {
            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            {
                context.OpenReadTransaction();

                var documents = Database.DocumentsStorage.GetDocumentsInReverseEtagOrder(context, GetStringQueryString("name"), GetStart(), GetPageSize());

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteDocuments(context, documents, metadataOnly: false);
                }
            }
            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/collections/$", "DELETE")]
        public Task Delete()
        {
            DocumentsOperationContext context;
            var returnContextToPool = ContextPool.AllocateOperationContext(out context);

            ExecuteCollectionOperation((runner, collectionName, options, onProgress, token) => Task.Run(()=>runner.ExecuteDelete(collectionName, options, context, onProgress, token)),
                context, returnContextToPool, DatabaseOperations.PendingOperationType.DeleteByCollection);
            return Task.CompletedTask;

        }

        [RavenAction("/databases/*/collections/$", "PATCH")]
        public Task Patch()
        {
            DocumentsOperationContext context;
            var returnContextToPool = ContextPool.AllocateOperationContext(out context);

            var reader = context.Read(RequestBodyStream(), "ScriptedPatchRequest");
            var patch = Documents.Patch.PatchRequest.Parse(reader);

            ExecuteCollectionOperation((runner, collectionName, options, onProgress, token) => Task.Run(() => runner.ExecutePatch(collectionName, options, patch, context, onProgress, token)),
                context, returnContextToPool, DatabaseOperations.PendingOperationType.DeleteByCollection);
            return Task.CompletedTask;

        }

        private void ExecuteCollectionOperation(Func<CollectionRunner, string, CollectionOperationOptions, Action<IOperationProgress>, OperationCancelToken, Task<IOperationResult>> operation, DocumentsOperationContext context, IDisposable returnContextToPool, DatabaseOperations.PendingOperationType operationType)
        {
            var collectionName = RouteMatch.Url.Substring(RouteMatch.MatchLength);

            var token = CreateTimeLimitedOperationToken();

            var collectionRunner = new CollectionRunner(Database, context);

            var operationId = Database.Operations.GetNextOperationId();

            var options = GetCollectionOperationOptions();

            var task = Database.Operations.AddOperation(collectionName, operationType, onProgress =>
                    operation(collectionRunner, collectionName, options ,onProgress, token), operationId, token);

            task.ContinueWith(_ => returnContextToPool.Dispose());

            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteOperationId(context, operationId);
            }
        }

        private CollectionOperationOptions GetCollectionOperationOptions()
        {
            return new CollectionOperationOptions
            {
                MaxOpsPerSecond = GetIntValueQueryString("maxOpsPerSec", required: false),
            };
        }
    }
}