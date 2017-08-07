using System;
using System.Diagnostics;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Raven.Server.NotificationCenter.Notifications.Details;

namespace Raven.Server.Documents.Handlers
{
    public class CollectionsHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/collections/stats", "GET", AuthorizationStatus.ValidUser)]
        public Task GetCollectionStats()
        {
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                var collections = new DynamicJsonValue();
                var result = new DynamicJsonValue
                {
                    [nameof(CollectionStatistics.CountOfDocuments)] = Database.DocumentsStorage.GetNumberOfDocuments(context),
                    [nameof(CollectionStatistics.CountOfConflicts)] = 
                        Database.DocumentsStorage.ConflictsStorage.GetCountOfDocumentsConflicts(context),
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

        [RavenAction("/databases/*/collections/docs", "GET", AuthorizationStatus.ValidUser)]
        public Task GetCollectionDocuments()
        {
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                var sw = Stopwatch.StartNew();
                var pageSize = GetPageSize();
                var documents = Database.DocumentsStorage.GetDocumentsInReverseEtagOrder(context, GetStringQueryString("name"), GetStart(), pageSize);

                int numberOfResults;
                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {

                    writer.WriteStartObject();
                    writer.WritePropertyName("Results");
                    writer.WriteDocuments(context, documents, metadataOnly: false, numberOfResults: out numberOfResults);
                    writer.WriteEndObject();
                }

                AddPagingPerformanceHint(PagingOperationType.Documents, "Collection", HttpContext, numberOfResults, pageSize, sw.Elapsed);
            }

            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/collections/docs", "DELETE", AuthorizationStatus.ValidUser)]
        public Task Delete()
        {
            var returnContextToPool = ContextPool.AllocateOperationContext(out DocumentsOperationContext context);

            ExecuteCollectionOperation((runner, collectionName, options, onProgress, token) => Task.Run(async () => await runner.ExecuteDelete(collectionName, options, onProgress, token)),
                context, returnContextToPool, Operations.Operations.OperationType.DeleteByCollection);
            return Task.CompletedTask;

        }

        [RavenAction("/databases/*/collections/docs", "PATCH", AuthorizationStatus.ValidUser)]
        public Task Patch()
        {
            var returnContextToPool = ContextPool.AllocateOperationContext(out DocumentsOperationContext context);

            var reader = context.Read(RequestBodyStream(), "ScriptedPatchRequest");
            var patch = Documents.Patch.PatchRequest.Parse(reader);

            ExecuteCollectionOperation((runner, collectionName, options, onProgress, token) => Task.Run(async () => await runner.ExecutePatch(collectionName, options, patch, onProgress, token)),
                context, returnContextToPool, Operations.Operations.OperationType.UpdateByCollection);
            return Task.CompletedTask;

        }

        private void ExecuteCollectionOperation(Func<CollectionRunner, string, CollectionOperationOptions, Action<IOperationProgress>, OperationCancelToken, Task<IOperationResult>> operation, DocumentsOperationContext context, IDisposable returnContextToPool, Operations.Operations.OperationType operationType)
        {
            var collectionName = GetStringQueryString("name");

            var token = CreateTimeLimitedOperationToken();

            var collectionRunner = new CollectionRunner(Database, context);

            var operationId = Database.Operations.GetNextOperationId();

            var options = GetCollectionOperationOptions();

            var task = Database.Operations.AddOperation(Database,collectionName, operationType, onProgress =>
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