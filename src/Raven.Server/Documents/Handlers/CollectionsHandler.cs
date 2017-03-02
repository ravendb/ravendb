using System;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Raven.Server.Documents.Operations;
using Raven.Server.NotificationCenter.Notifications;

namespace Raven.Server.Documents.Handlers
{
    public class CollectionsHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/collections/stats", "GET", "/databases/{databaseName:string}/collections/stats")]
        public Task GetCollectionStats()
        {
            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            using (context.OpenReadTransaction())
            {
                var collections = new DynamicJsonValue();
                var result = new DynamicJsonValue
                {
                    [nameof(CollectionStatistics.CountOfDocuments)] = Database.DocumentsStorage.GetNumberOfDocuments(context),
                    [nameof(CollectionStatistics.Collections)] = collections
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
            using (context.OpenReadTransaction())
            {

                long collectionCount;
                //TODO: remove one we implement doc-preview endpoint - RavenDB-6466
                {
                    var collectionName = GetStringQueryString("name");
                    var lastCollectionEtag = Database.DocumentsStorage.GetLastDocumentEtag(context, collectionName);
                    
                    collectionCount = Database.DocumentsStorage.GetCollection(collectionName, context).Count;
                    var actualEtag = DatabaseStatsChanged.ComputeEtag(lastCollectionEtag, collectionCount);

                    HttpContext.Response.Headers["ETag"] = "\"" + actualEtag + "\"";
                }

                var documents = Database.DocumentsStorage.GetDocumentsInReverseEtagOrder(context, GetStringQueryString("name"), GetStart(), GetPageSize(Database.Configuration.Core.MaxPageSize));

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartObject();

                    { //TODO: delete me - once doc-preview will be finished - RavenDB-6466
                        writer.WritePropertyName("totalResultCount"); 
                        writer.WriteInteger(collectionCount);
                        writer.WriteComma();
                    }
                    
                    writer.WritePropertyName("Results");
                    writer.WriteDocuments(context, documents, metadataOnly: false);
                    writer.WriteEndObject();
                }
            }

            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/collections/docs", "DELETE")]
        public Task Delete()
        {
            DocumentsOperationContext context;
            var returnContextToPool = ContextPool.AllocateOperationContext(out context);

            ExecuteCollectionOperation((runner, collectionName, options, onProgress, token) => Task.Run(() => runner.ExecuteDelete(collectionName, options, context, onProgress, token)),
                context, returnContextToPool, DatabaseOperations.OperationType.DeleteByCollection);
            return Task.CompletedTask;

        }

        [RavenAction("/databases/*/collections/docs", "PATCH")]
        public Task Patch()
        {
            DocumentsOperationContext context;
            var returnContextToPool = ContextPool.AllocateOperationContext(out context);

            var reader = context.Read(RequestBodyStream(), "ScriptedPatchRequest");
            var patch = Documents.Patch.PatchRequest.Parse(reader);

            ExecuteCollectionOperation((runner, collectionName, options, onProgress, token) => Task.Run(() => runner.ExecutePatch(collectionName, options, patch, context, onProgress, token)),
                context, returnContextToPool, DatabaseOperations.OperationType.UpdateByCollection);
            return Task.CompletedTask;

        }

        private void ExecuteCollectionOperation(Func<CollectionRunner, string, CollectionOperationOptions, Action<IOperationProgress>, OperationCancelToken, Task<IOperationResult>> operation, DocumentsOperationContext context, IDisposable returnContextToPool, DatabaseOperations.OperationType operationType)
        {
            var collectionName = GetStringQueryString("name");

            var token = CreateTimeLimitedOperationToken();

            var collectionRunner = new CollectionRunner(Database, context);

            var operationId = Database.Operations.GetNextOperationId();

            var options = GetCollectionOperationOptions();

            var task = Database.Operations.AddOperation(collectionName, operationType, onProgress =>
                    operation(collectionRunner, collectionName, options, onProgress, token), operationId, token);

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