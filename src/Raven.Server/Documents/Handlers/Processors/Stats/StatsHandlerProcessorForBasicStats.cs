using System.Linq;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations;
using Raven.Server.Documents.Indexes;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.Stats;

internal class StatsHandlerProcessorForBasicStats : AbstractStatsHandlerProcessorForBasicStats<DatabaseRequestHandler, DocumentsOperationContext>
{
    public StatsHandlerProcessorForBasicStats([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override ValueTask<BasicDatabaseStatistics> GetBasicDatabaseStatisticsAsync(DocumentsOperationContext context)
    {
        using (context.OpenReadTransaction())
        {
            var stats = new BasicDatabaseStatistics();

            FillBasicDatabaseStatistics(stats, context, RequestHandler.Database);

            return ValueTask.FromResult(stats);
        }
    }

    internal static void FillBasicDatabaseStatistics(BasicDatabaseStatistics stats, DocumentsOperationContext context, DocumentDatabase database)
    {
        var indexes = database.IndexStore.GetIndexes().ToList();

        stats.CountOfDocuments = database.DocumentsStorage.GetNumberOfDocuments(context);
        stats.CountOfRevisionDocuments = database.DocumentsStorage.RevisionsStorage.GetNumberOfRevisionDocuments(context);
        stats.CountOfDocumentsConflicts = database.DocumentsStorage.ConflictsStorage.GetNumberOfDocumentsConflicts(context);
        stats.CountOfTombstones = database.DocumentsStorage.GetNumberOfTombstones(context);
        stats.CountOfConflicts = database.DocumentsStorage.ConflictsStorage.ConflictsCount;

        stats.CountOfCounterEntries = database.DocumentsStorage.CountersStorage.GetNumberOfCounterEntries(context);

        stats.CountOfTimeSeriesSegments = database.DocumentsStorage.TimeSeriesStorage.GetNumberOfTimeSeriesSegments(context);

        var attachments = database.DocumentsStorage.AttachmentsStorage.GetNumberOfAttachments(context);
        stats.CountOfAttachments = attachments.AttachmentCount;
        stats.CountOfIndexes = indexes.Count;

        stats.Indexes = new BasicIndexInformation[indexes.Count];
        for (var i = 0; i < indexes.Count; i++)
        {
            var index = indexes[i].ToIndexInformationHolder();
            stats.Indexes[i] = index.ToBasicIndexInformation();
        }
    }
}
