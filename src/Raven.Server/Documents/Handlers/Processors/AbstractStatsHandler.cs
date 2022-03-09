using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Sharding;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors
{
    internal abstract class AbstractStatsHandler<TRequestHandler, TOperationContext> : AbstractHandlerProcessor<TRequestHandler, TOperationContext>
        where TRequestHandler : RequestHandler
        where TOperationContext : JsonOperationContext
    {
        protected AbstractStatsHandler([NotNull] TRequestHandler requestHandler, [NotNull] JsonContextPoolBase<TOperationContext> contextPool) : base(requestHandler, contextPool)
        {
        }

        protected void FillDatabaseStatistics(DatabaseStatistics stats, QueryOperationContext context)
        {
            if (RequestHandler is DatabaseRequestHandler databaseRequestHandler)
            {
                var indexes = databaseRequestHandler.Database.IndexStore.GetIndexes().ToList();
                var size = databaseRequestHandler.Database.GetSizeOnDisk();

                stats.LastDocEtag = DocumentsStorage.ReadLastDocumentEtag(context.Documents.Transaction.InnerTransaction);
                stats.LastDatabaseEtag = DocumentsStorage.ReadLastEtag(context.Documents.Transaction.InnerTransaction);
                stats.DatabaseChangeVector = DocumentsStorage.GetDatabaseChangeVector(context.Documents);

                stats.CountOfDocuments = databaseRequestHandler.Database.DocumentsStorage.GetNumberOfDocuments(context.Documents);
                stats.CountOfRevisionDocuments = databaseRequestHandler.Database.DocumentsStorage.RevisionsStorage.GetNumberOfRevisionDocuments(context.Documents);
                stats.CountOfDocumentsConflicts = databaseRequestHandler.Database.DocumentsStorage.ConflictsStorage.GetNumberOfDocumentsConflicts(context.Documents);
                stats.CountOfTombstones = databaseRequestHandler.Database.DocumentsStorage.GetNumberOfTombstones(context.Documents);
                stats.CountOfConflicts = databaseRequestHandler.Database.DocumentsStorage.ConflictsStorage.ConflictsCount;
                stats.SizeOnDisk = size.Data;
                stats.NumberOfTransactionMergerQueueOperations = databaseRequestHandler.Database.TxMerger.NumberOfQueuedOperations;
                stats.TempBuffersSizeOnDisk = size.TempBuffers;
                stats.CountOfCounterEntries = databaseRequestHandler.Database.DocumentsStorage.CountersStorage.GetNumberOfCounterEntries(context.Documents);

                stats.CountOfTimeSeriesSegments = databaseRequestHandler.Database.DocumentsStorage.TimeSeriesStorage.GetNumberOfTimeSeriesSegments(context.Documents);

                var attachments = databaseRequestHandler.Database.DocumentsStorage.AttachmentsStorage.GetNumberOfAttachments(context.Documents);
                stats.CountOfAttachments = attachments.AttachmentCount;
                stats.CountOfUniqueAttachments = attachments.StreamsCount;
                stats.CountOfIndexes = indexes.Count;

                stats.DatabaseId = databaseRequestHandler.Database.DocumentsStorage.Environment.Base64Id;
                stats.Is64Bit = !databaseRequestHandler.Database.DocumentsStorage.Environment.Options.ForceUsing32BitsPager && IntPtr.Size == sizeof(long);
                stats.Pager = databaseRequestHandler.Database.DocumentsStorage.Environment.Options.DataPager.GetType().ToString();

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
        }

        protected IndexInformation[] GetDatabaseIndexesFromRecord()
        {
            if (RequestHandler is ShardedRequestHandler shardedRequestHandler)
            {
                var record = shardedRequestHandler.ShardedContext.DatabaseRecord;
                var indexes = record.Indexes;
                var indexInformation = new IndexInformation[indexes.Count];

                int i = 0;
                foreach (var key in indexes.Keys)
                {
                    var index = indexes[key];

                    indexInformation[i] = new IndexInformation
                    {
                        Name = index.Name,
                        // IndexDefinition includes nullable fields, then in case of null we set to default values
                        State = index.State ?? IndexState.Normal,
                        LockMode = index.LockMode ?? IndexLockMode.Unlock,
                        Priority = index.Priority ?? IndexPriority.Normal,
                        Type = index.Type,
                        SourceType = index.SourceType,
                        IsStale = false // for sharding we can't determine 
                    };

                    i++;
                }

                return indexInformation;
            }

            return null;
        }
    }
}
