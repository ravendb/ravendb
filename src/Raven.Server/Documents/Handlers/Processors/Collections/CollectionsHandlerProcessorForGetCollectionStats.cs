using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations;
using Raven.Server.ServerWide.Context;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers.Processors.Collections
{
    internal class CollectionsHandlerProcessorForGetCollectionStats : AbstractCollectionsHandlerProcessorForGetCollectionStats<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public CollectionsHandlerProcessorForGetCollectionStats([NotNull] DatabaseRequestHandler requestHandler, bool detailed) : base(requestHandler, detailed)
        {
        }

        protected override ValueTask<DynamicJsonValue> GetStatsAsync(DocumentsOperationContext context, bool detailed)
        {
            using (context.OpenReadTransaction())
            {
                return ValueTask.FromResult(GetCollectionStats(context, detailed));
            }
        }

        private DynamicJsonValue GetCollectionStats(DocumentsOperationContext context, bool detailed = false)
        {
            DynamicJsonValue collections = new DynamicJsonValue();

            DynamicJsonValue stats = new DynamicJsonValue()
            {
                [nameof(CollectionStatistics.CountOfDocuments)] = RequestHandler.Database.DocumentsStorage.GetNumberOfDocuments(context),
                [nameof(CollectionStatistics.CountOfConflicts)] = RequestHandler.Database.DocumentsStorage.ConflictsStorage.GetNumberOfDocumentsConflicts(context),
                [nameof(CollectionStatistics.Collections)] = collections
            };

            foreach (var collection in RequestHandler.Database.DocumentsStorage.GetCollections(context))
            {
                if (detailed)
                {
                    collections[collection.Name] = RequestHandler.Database.DocumentsStorage.GetCollectionDetails(context, collection.Name);
                }
                else
                {
                    collections[collection.Name] = collection.Count;
                }
            }

            return stats;
        }
    }
}
