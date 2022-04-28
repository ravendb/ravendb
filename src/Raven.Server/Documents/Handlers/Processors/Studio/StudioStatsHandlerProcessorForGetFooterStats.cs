using System.Linq;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Studio;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.Studio
{
    internal class StudioStatsHandlerProcessorForGetFooterStats : AbstractStudioStatsHandlerProcessorForGetFooterStats<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public StudioStatsHandlerProcessorForGetFooterStats([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override ValueTask<FooterStatistics> GetFooterStatisticsAsync()
        {
            var indexes = RequestHandler.Database.IndexStore.GetIndexes().ToList();

            using (var context = QueryOperationContext.Allocate(RequestHandler.Database, needsServerContext: true))
            using (context.OpenReadTransaction())
            {
                return ValueTask.FromResult(new FooterStatistics()
                {
                    CountOfDocuments = RequestHandler.Database.DocumentsStorage.GetNumberOfDocuments(context.Documents),
                    CountOfIndexes = indexes.Count,
                    CountOfStaleIndexes = indexes.Count(i => i.IsStale(context)),
                    CountOfIndexingErrors = indexes.Sum(index => index.GetErrorCount())
                });
            }
        }
    }
}
