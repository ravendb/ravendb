using System.Collections.Generic;
using System.Threading.Tasks;

using Raven.Server.Json;
using Raven.Server.Routing;
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