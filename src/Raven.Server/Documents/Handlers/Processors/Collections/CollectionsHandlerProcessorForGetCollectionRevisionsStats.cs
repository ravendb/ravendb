using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations;
using Raven.Server.ServerWide.Context;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers.Processors.Collections
{
    internal class CollectionsHandlerProcessorForGetCollectionRevisionsStats : AbstractCollectionsHandlerProcessorForGetCollectionRevisionsStats<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public CollectionsHandlerProcessorForGetCollectionRevisionsStats([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override ValueTask<DynamicJsonValue> GetStatsAsync(DocumentsOperationContext context)
        {
            using (context.OpenReadTransaction())
            {
                return ValueTask.FromResult(GetCollectionStats(context));
            }
        }

        private DynamicJsonValue GetCollectionStats(DocumentsOperationContext context)
        {
            var collections = new DynamicJsonValue();

            foreach (var collection in RequestHandler.Database.DocumentsStorage.GetCollectionsNames(context))
            {
                collections[collection] = RequestHandler.Database.DocumentsStorage.RevisionsStorage.GetNumberOfRevisionDocumentsForCollection(context, collection);
            }

            return new DynamicJsonValue()
            {
                [nameof(CollectionRevisionsStatistics.CountOfRevisions)] = RequestHandler.Database.DocumentsStorage.RevisionsStorage.GetNumberOfRevisionDocuments(context),
                [nameof(CollectionRevisionsStatistics.Collections)] = collections
            };
        }
    }
}
