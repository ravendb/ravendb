using System.Collections.Generic;
using System.Threading;
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

        protected override ValueTask<DynamicJsonValue> GetStatsAsync(DocumentsOperationContext context, CancellationToken token)
        {
            using (context.OpenReadTransaction())
            {
                return ValueTask.FromResult(GetCollectionStats(context));
            }
        }

        private DynamicJsonValue GetCollectionStats(DocumentsOperationContext context)
        {
            var result = new CollectionRevisionsStatistics
            {
                Collections = new Dictionary<string, long>(),
                CountOfRevisions = RequestHandler.Database.DocumentsStorage.RevisionsStorage.GetNumberOfRevisionDocuments(context)
            };

            foreach (var collection in RequestHandler.Database.DocumentsStorage.GetCollectionsNames(context))
            {
                var count = RequestHandler.Database.DocumentsStorage.RevisionsStorage.GetNumberOfRevisionDocumentsForCollection(context, collection);
                result.Collections.Add(collection, count);
            }

            return result.ToJson();
        }
    }
}
