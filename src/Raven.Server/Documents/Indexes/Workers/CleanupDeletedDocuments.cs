using System;
using System.Diagnostics;
using System.Threading;
using Raven.Abstractions.Logging;
using Raven.Server.Config.Categories;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Indexes.Workers
{
    public class CleanupDeletedDocuments : IIndexingWork
    {
        protected readonly ILog Log = LogManager.GetLogger(typeof(CleanupDeletedDocuments));
        
        private readonly Index _index;
        private readonly IndexingConfiguration _configuration;
        private readonly DocumentsStorage _documentsStorage;
        private readonly IndexStorage _indexStorage;

        public CleanupDeletedDocuments(Index index, DocumentsStorage documentsStorage, IndexStorage indexStorage, IndexingConfiguration configuration)
        {
            _index = index;
            _configuration = configuration;
            _documentsStorage = documentsStorage;
            _indexStorage = indexStorage;
        }

        public bool Execute(DocumentsOperationContext databaseContext, TransactionOperationContext indexContext, 
                            Lazy<IndexWriteOperation> writeOperation, IndexingBatchStats stats, CancellationToken token)
        {
            var pageSize = _configuration.MaxNumberOfTombstonesToFetch;
            var timeoutProcessing = Debugger.IsAttached == false ? _configuration.DocumentProcessingTimeout.AsTimeSpan : TimeSpan.FromMinutes(15);

            var moreWorkFound = false;

            foreach (var collection in _index.Collections)
            {
                if (Log.IsDebugEnabled)
                    Log.Debug($"Executing cleanup for '{_index.Name} ({_index.IndexId})'. Collection: {collection}.");

                var lastMappedEtag = _indexStorage.ReadLastMappedEtag(indexContext.Transaction, collection);
                var lastTombstoneEtag = _indexStorage.ReadLastProcessedTombstoneEtag(indexContext.Transaction, collection);

                if (Log.IsDebugEnabled)
                    Log.Debug($"Executing cleanup for '{_index} ({_index.Name})'. LastMappedEtag: {lastMappedEtag}. LastTombstoneEtag: {lastTombstoneEtag}.");

                var lastEtag = lastTombstoneEtag;
                var count = 0;

                var sw = Stopwatch.StartNew();
                IndexWriteOperation indexWriter = null;

                using (databaseContext.OpenReadTransaction())
                {
                    foreach (var tombstone in _documentsStorage.GetTombstonesAfter(databaseContext, collection, lastEtag + 1, 0, pageSize))
                    {
                        token.ThrowIfCancellationRequested();

                        if (indexWriter == null)
                            indexWriter = writeOperation.Value;
                        
                        if (Log.IsDebugEnabled)
                            Log.Debug($"Executing cleanup for '{_index} ({_index.Name})'. Processing tombstone {tombstone.Key} ({tombstone.Etag}).");

                        count++;
                        lastEtag = tombstone.Etag;

                        if (tombstone.DeletedEtag > lastMappedEtag)
                            continue; // no-op, we have not yet indexed this document

                        _index.HandleDelete(tombstone, indexWriter, indexContext);
                        
                        if (sw.Elapsed > timeoutProcessing)
                        {
                            break;
                        }
                    }
                }

                if (count == 0)
                    continue;

                if (Log.IsDebugEnabled)
                    Log.Debug($"Executing cleanup for '{_index} ({_index.Name})'. Processed {count} tombstones in '{collection}' collection in {sw.ElapsedMilliseconds:#,#;;0} ms.");

                if (lastEtag <= lastTombstoneEtag)
                    continue;

                _indexStorage.WriteLastTombstoneEtag(indexContext.Transaction, collection, lastEtag);

                moreWorkFound = true;
            }

            return moreWorkFound;
        }

        public void Dispose()
        {
        }
    }
}