using System;
using System.Linq;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations;
using Raven.Server.Documents.Indexes;
using Raven.Server.Json;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Server.Web.Http;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Stats
{
    internal sealed class StatsHandlerProcessorForGetDatabaseStatistics : AbstractStatsHandlerProcessorForGetDatabaseStatistics<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public StatsHandlerProcessorForGetDatabaseStatistics([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override bool SupportsCurrentNode => true;

        protected override ValueTask HandleCurrentNodeAsync()
        {
            using (var context = QueryOperationContext.Allocate(RequestHandler.Database, needsServerContext: true))
            using (context.OpenReadTransaction())
            {
                var stats = new DatabaseStatistics();

                FillDatabaseStatistics(stats, context, RequestHandler.Database);

                return WriteResultAsync(stats);
            }
        }

        protected override Task HandleRemoteNodeAsync(ProxyCommand<DatabaseStatistics> command, OperationCancelToken token) => RequestHandler.ExecuteRemoteAsync(command, token.Token);

        internal static void FillDatabaseStatistics(DatabaseStatistics stats, QueryOperationContext context, DocumentDatabase database)
        {
            var indexes = database.IndexStore.GetIndexes().ToList();
            var size = database.GetSizeOnDisk();

            stats.LastDocEtag = DocumentsStorage.ReadLastDocumentEtag(context.Documents.Transaction.InnerTransaction);
            stats.LastDatabaseEtag = database.DocumentsStorage.ReadLastEtag(context.Documents.Transaction.InnerTransaction);
            stats.DatabaseChangeVector = DocumentsStorage.GetDatabaseChangeVector(context.Documents);

            stats.CountOfDocuments = database.DocumentsStorage.GetNumberOfDocuments(context.Documents);
            stats.CountOfRevisionDocuments = database.DocumentsStorage.RevisionsStorage.GetNumberOfRevisionDocuments(context.Documents);
            stats.CountOfDocumentsConflicts = database.DocumentsStorage.ConflictsStorage.GetNumberOfDocumentsConflicts(context.Documents);
            stats.CountOfTombstones = database.DocumentsStorage.GetNumberOfTombstones(context.Documents);
            stats.CountOfConflicts = database.DocumentsStorage.ConflictsStorage.ConflictsCount;
            stats.SizeOnDisk = size.Data;
            stats.NumberOfTransactionMergerQueueOperations = database.TxMerger.NumberOfQueuedOperations;
            stats.TempBuffersSizeOnDisk = size.TempBuffers;
            stats.CountOfCounterEntries = database.DocumentsStorage.CountersStorage.GetNumberOfCounterEntries(context.Documents);

            stats.CountOfTimeSeriesSegments = database.DocumentsStorage.TimeSeriesStorage.GetNumberOfTimeSeriesSegments(context.Documents);

            var attachments = database.DocumentsStorage.AttachmentsStorage.GetNumberOfAttachments(context.Documents);
            stats.CountOfAttachments = attachments.AttachmentCount;
            stats.CountOfUniqueAttachments = attachments.StreamsCount;
            stats.CountOfIndexes = indexes.Count;

            stats.DatabaseId = database.DocumentsStorage.Environment.Base64Id;
            stats.Is64Bit = database.DocumentsStorage.Environment.Options.ForceUsing32BitsPager == false && IntPtr.Size == sizeof(long);
            stats.Pager = database.DocumentsStorage.Environment.DataPager.GetType().ToString();

            stats.Indexes = new IndexInformation[indexes.Count];
            for (var i = 0; i < indexes.Count; i++)
            {
                var index = indexes[i];
                bool isStale;
                try
                {
                    isStale = index.IsStale(context);
                }
                catch (OperationCanceledException)
                {
                    // if the index has just been removed, let us consider it stale
                    // until it can be safely removed from the list of indexes in the
                    // database
                    isStale = true;
                }
                stats.Indexes[i] = new IndexInformation
                {
                    State = index.State,
                    IsStale = isStale,
                    Name = index.Name,
                    LockMode = index.Definition.LockMode,
                    ArchivedDataProcessingBehavior = index.ArchivedDataProcessingBehavior,
                    Priority = index.Definition.Priority,
                    Type = index.Type,
                    LastIndexingTime = index.LastIndexingTime,
                    SourceType = index.SourceType
                };

                if (stats.LastIndexingTime.HasValue)
                    stats.LastIndexingTime = stats.LastIndexingTime >= index.LastIndexingTime ? stats.LastIndexingTime : index.LastIndexingTime;
                else
                    stats.LastIndexingTime = index.LastIndexingTime;
            }
        }

        private async ValueTask WriteResultAsync(DatabaseStatistics result)
        {
            using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriterForDebug(context, ServerStore, RequestHandler.ResponseBodyStream()))
                writer.WriteDatabaseStatistics(context, result);
        }
    }
}
