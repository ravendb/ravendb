using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations;
using Raven.Server.ServerWide.Context;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers.Processors.Collections
{
    internal sealed class CollectionsHandlerProcessorForGetCollectionStats : AbstractCollectionsHandlerProcessorForGetCollectionStats<DatabaseRequestHandler, DocumentsOperationContext>
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
                [nameof(CollectionStatistics.CountOfDocumentsConflicts)] = RequestHandler.Database.DocumentsStorage.ConflictsStorage.GetNumberOfDocumentsConflicts(context),
                [nameof(CollectionStatistics.CountOfConflicts)] = RequestHandler.Database.DocumentsStorage.ConflictsStorage.ConflictsCount,
                [nameof(CollectionStatistics.CountOfRevisionDocuments)] = RequestHandler.Database.DocumentsStorage.RevisionsStorage.GetNumberOfRevisionDocuments(context),
                [nameof(CollectionStatistics.CountOfTombstones)] = RequestHandler.Database.DocumentsStorage.GetNumberOfTombstones(context),
                [nameof(CollectionStatistics.CountOfTimeSeriesDeletedRanges)] = RequestHandler.Database.DocumentsStorage.TimeSeriesStorage.GetNumberOfTimeSeriesDeletedRanges(context),
                [nameof(CollectionStatistics.CountOfAttachments)] = RequestHandler.Database.DocumentsStorage.AttachmentsStorage.GetNumberOfAttachments(context).AttachmentCount,
                [nameof(CollectionStatistics.CountOfCounterEntries)] = RequestHandler.Database.DocumentsStorage.CountersStorage.GetNumberOfCounterEntries(context),
                [nameof(CollectionStatistics.CountOfTimeSeriesSegments)] = RequestHandler.Database.DocumentsStorage.TimeSeriesStorage.GetNumberOfTimeSeriesSegments(context),
                [nameof(CollectionStatistics.Collections)] = collections,
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
