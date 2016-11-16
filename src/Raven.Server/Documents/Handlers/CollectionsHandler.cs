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

            ExecuteCollectionOperation((runner, collectionName, onProgress, token) => Task.Run(()=>runner.ExecuteDelete(collectionName, context, onProgress, token)),
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

            ExecuteCollectionOperation((runner, collectionName, onProgress, token) => Task.Run(() => runner.ExecutePatch(collectionName, patch, context, onProgress, token)),
                context, returnContextToPool, DatabaseOperations.PendingOperationType.DeleteByCollection);
            return Task.CompletedTask;

        }

        private void ExecuteCollectionOperation(Func<CollectionRunner, string, Action<IOperationProgress>, OperationCancelToken, Task<IOperationResult>> operation, DocumentsOperationContext context, IDisposable returnContextToPool, DatabaseOperations.PendingOperationType operationType)
        {
            var collectionName = RouteMatch.Url.Substring(RouteMatch.MatchLength);

            var token = CreateTimeLimitedOperationToken();

            var collectionRunner = new CollectionRunner(Database, context);

            var operationId = Database.Operations.GetNextOperationId();

            var task = Database.Operations.AddOperation(collectionName, operationType, onProgress =>
                    operation(collectionRunner, collectionName, onProgress, token), operationId, token);

            task.ContinueWith(_ => returnContextToPool.Dispose());

            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteStartObject();
                writer.WritePropertyName("OperationId");
                writer.WriteInteger(operationId);
                writer.WriteEndObject();
            }
        }

        [RavenAction("/databases/*/collections/docs", "DELETE", "/databases/{databaseName:string}/collections/docs?name={collectionName:string}")]
        public Task DeleteCollectionDocuments()
        {
            var deletedList = new List<LazyStringValue>();
            long totalDocsDeletes = 0;
            long maxEtag = -1;
            DocumentsOperationContext context;
            var collection = GetStringQueryString("name");
            using (ContextPool.AllocateOperationContext(out context))
            {
                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartArray();
                    while (true)
                    {
                        using (context.OpenWriteTransaction())
                        {
                            if (maxEtag == -1)
                                maxEtag = DocumentsStorage.ReadLastEtag(context.Transaction.InnerTransaction);

                            foreach (var document in Database.DocumentsStorage.GetDocumentsFrom(context, collection, 0, 0, 16 * 1024))
                            {
                                if (document.Etag > maxEtag)
                                    break;
                                deletedList.Add(document.Key);
                            }

                            if (deletedList.Count == 0)
                                break;

                            foreach (LazyStringValue key in deletedList)
                            {
                                Database.DocumentsStorage.Delete(context, key, null);
                            }

                            context.Transaction.Commit();
                        }
                        context.Write(writer, new DynamicJsonValue
                        {
                            ["BatchSize"] = deletedList.Count
                        });
                        writer.WriteComma();
                        writer.WriteNewLine();
                        writer.Flush();

                        totalDocsDeletes += deletedList.Count;

                        deletedList.Clear();
                    }
                    context.Write(writer, new DynamicJsonValue
                    {
                        ["TotalDocsDeleted"] = totalDocsDeletes
                    });
                    writer.WriteNewLine();
                    writer.WriteEndArray();
                }
            }
            return Task.CompletedTask;
        }
    }
}