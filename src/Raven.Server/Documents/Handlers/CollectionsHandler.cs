using System.Diagnostics;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations;
using Raven.Server.Json;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers
{
    public class CollectionsHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/collections/stats", "GET", AuthorizationStatus.ValidUser)]
        public Task GetCollectionStats()
        {
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                CollectionStatistics collectionStatistics = new CollectionStatistics();

                FillCollectionStats(collectionStatistics, context);

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                    context.Write(writer, collectionStatistics.ToJson());
            }

            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/collections/stats/detailed", "GET", AuthorizationStatus.ValidUser)]
        public Task GetDetailedCollectionStats()
        {
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                DetailedCollectionStatistics detailedCollectionStatistics = new DetailedCollectionStatistics();

                FillCollectionStats(detailedCollectionStatistics, context);

                using (context.OpenReadTransaction())
                {
                    foreach (var collection in detailedCollectionStatistics.Collections)
                    {
                        detailedCollectionStatistics.ExtendedCollectionDetails[collection.Key] = Database.DocumentsStorage.GetCollectionDetails(collection, context);
                    }
                }

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                    context.Write(writer, detailedCollectionStatistics.ToJson());
            }

            return Task.CompletedTask;
        }

        private void FillCollectionStats(CollectionStatistics stats, DocumentsOperationContext context)
        {
            using (context.OpenReadTransaction())
            {
                stats.CountOfDocuments = Database.DocumentsStorage.GetNumberOfDocuments(context);
                stats.CountOfConflicts = Database.DocumentsStorage.ConflictsStorage.GetNumberOfDocumentsConflicts(context);

                foreach (var collectionStat in Database.DocumentsStorage.GetCollections(context))
                {
                    stats.Collections[collectionStat.Name] = collectionStat.Count;
                }
            }
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

                AddPagingPerformanceHint(PagingOperationType.Documents, "Collection", HttpContext.Request.QueryString.Value, numberOfResults, pageSize, sw.ElapsedMilliseconds);
            }

            return Task.CompletedTask;
        }
    }
}
