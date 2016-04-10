using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;

using Microsoft.AspNet.Http;

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
                WriteDocuments(context, documents);
            }
            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/collections/docs", "DELETE", "/databases/{databaseName:string}/collections/docs?name={collectionName:string}")]
        public Task DeleteCollectionDocuments()
        {
            var deletedList = new List<long>();
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
                        bool isAllDeleted;
                        using (context.OpenWriteTransaction())
                        {
                            if (maxEtag == -1)
                                maxEtag = DocumentsStorage.ReadLastEtag(context.Transaction.InnerTransaction);

                            isAllDeleted = Database.DocumentsStorage.DeleteCollection(context, collection, deletedList, maxEtag);
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

                        if (isAllDeleted)
                            break;

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