using System;
using System.Diagnostics;
using System.Threading;

using Raven.Abstractions.Logging;
using Raven.Client.Data.Indexes;
using Raven.Server.ServerWide.Context;
using Voron;

namespace Raven.Server.Documents.Indexes.Auto
{
    public class AutoMapIndex : Index<AutoIndexDefinition>
    {
        private AutoMapIndex(int indexId, AutoIndexDefinition definition)
            : base(indexId, IndexType.AutoMap, definition)
        {
        }

        public static AutoMapIndex CreateNew(int indexId, AutoIndexDefinition definition, DocumentDatabase documentDatabase)
        {
            var instance = new AutoMapIndex(indexId, definition);
            instance.Initialize(documentDatabase);

            return instance;
        }

        public static AutoMapIndex Open(int indexId, StorageEnvironment environment, DocumentDatabase documentDatabase)
        {
            var definition = AutoIndexDefinition.Load(environment);
            var instance = new AutoMapIndex(indexId, definition);
            instance.Initialize(environment, documentDatabase);

            return instance;
        }

        public override void DoIndexingWork(IndexingBatchStats stats, CancellationToken cancellationToken)
        {
            DocumentsOperationContext databaseContext;
            TransactionOperationContext indexContext;

            using (DocumentDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out databaseContext))
            using (_contextPool.AllocateOperationContext(out indexContext))
            using (var tx = indexContext.OpenWriteTransaction())
            {
                ExecuteCleanup(cancellationToken, databaseContext, indexContext);
                ExecuteMap(stats, cancellationToken, databaseContext, indexContext);

                tx.Commit();
            }
        }

        private void ExecuteCleanup(CancellationToken token, DocumentsOperationContext databaseContext, TransactionOperationContext indexContext)
        {
            var pageSize = DocumentDatabase.Configuration.Indexing.MaxNumberOfTombstonesToFetch;

            if (Log.IsDebugEnabled)
                Log.Debug($"Executing cleanup for '{Name} ({IndexId})'. Collections: {string.Join(",", Collections)}.");

            foreach (var collection in Collections)
            {
                if (Log.IsDebugEnabled)
                    Log.Debug($"Executing cleanup for '{Name} ({IndexId})'. Collection: {collection}.");

                long lastMappedEtag;
                long lastTombstoneEtag;
                lastMappedEtag = ReadLastMappedEtag(indexContext.Transaction, collection);
                lastTombstoneEtag = ReadLastTombstoneEtag(indexContext.Transaction, collection);

                if (Log.IsDebugEnabled)
                    Log.Debug($"Executing cleanup for '{Name} ({IndexId})'. LastMappedEtag: {lastMappedEtag}. LastTombstoneEtag: {lastTombstoneEtag}.");

                var lastEtag = lastTombstoneEtag;
                var count = 0;

                using (var indexActions = IndexPersistence.OpenIndexWriter(indexContext.Transaction.InnerTransaction))
                {
                    using (databaseContext.OpenReadTransaction())
                    {
                        var sw = Stopwatch.StartNew();
                        foreach (var tombstone in DocumentDatabase.DocumentsStorage.GetTombstonesAfter(databaseContext, collection, lastEtag + 1, 0, pageSize))
                        {
                            token.ThrowIfCancellationRequested();

                            if (Log.IsDebugEnabled)
                                Log.Debug($"Executing cleanup for '{Name} ({IndexId})'. Processing tombstone: {tombstone.Key}.");

                            count++;
                            lastEtag = tombstone.Etag;

                            if (tombstone.DeletedEtag > lastMappedEtag)
                                continue; // no-op, we have not yet indexed this document

                            indexActions.Delete(tombstone.Key);

                            if (sw.Elapsed > DocumentDatabase.Configuration.Indexing.TombstoneProcessingTimeout.AsTimeSpan)
                            {
                                break;
                            }
                        }
                    }
                }

                if (count == 0)
                    return;

                if (Log.IsDebugEnabled)
                    Log.Debug($"Executing cleanup for '{Name} ({IndexId})'. Processed {count} tombstones in '{collection}' collection.");

                if (lastEtag <= lastTombstoneEtag)
                    return;

                WriteLastTombstoneEtag(indexContext.Transaction, collection, lastEtag);

                _mre.Set(); // might be more
            }
        }


        private void ExecuteMap(IndexingBatchStats stats, CancellationToken cancellationToken, DocumentsOperationContext databaseContext, TransactionOperationContext indexContext)
        {
            var pageSize = DocumentDatabase.Configuration.Indexing.MaxNumberOfDocumentsToFetchForMap;

            if (Log.IsDebugEnabled)
                Log.Debug($"Executing map for '{Name} ({IndexId})'. Collections: {string.Join(",", Collections)}.");

            foreach (var collection in Collections)
            {
                if (Log.IsDebugEnabled)
                    Log.Debug($"Executing map for '{Name} ({IndexId})'. Collection: {collection}.");

                long lastMappedEtag;
                lastMappedEtag = ReadLastMappedEtag(indexContext.Transaction, collection);

                if (Log.IsDebugEnabled)
                    Log.Debug($"Executing map for '{Name} ({IndexId})'. LastMappedEtag: {lastMappedEtag}.");

                var lastEtag = lastMappedEtag;
                var count = 0;

                using (var indexWriter = IndexPersistence.OpenIndexWriter(indexContext.Transaction.InnerTransaction))
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    using (databaseContext.OpenReadTransaction())
                    {
                        var sw = Stopwatch.StartNew();
                        foreach (var document in DocumentDatabase.DocumentsStorage.GetDocumentsAfter(databaseContext, collection, lastEtag + 1, 0, pageSize))
                        {
                            cancellationToken.ThrowIfCancellationRequested();

                            if (Log.IsDebugEnabled)
                                Log.Debug($"Executing cleanup for '{Name} ({IndexId})'. Processing document: {document.Key}.");

                            stats.IndexingAttempts++;

                            count++;
                            lastEtag = document.Etag;

                            try
                            {
                                indexWriter.IndexDocument(document);
                                stats.IndexingSuccesses++;
                                DocumentDatabase.Metrics.IndexedPerSecond.Mark();
                            }
                            catch (Exception e)
                            {
                                stats.IndexingErrors++;

                                Log.WarnException($"Failed to execute mapping function on '{document.Key}' for '{Name}'.", e);

                                //context.AddError // TODO [ppekrol]
                            }

                            if (sw.Elapsed > DocumentDatabase.Configuration.Indexing.DocumentProcessingTimeout.AsTimeSpan)
                            {
                                break;
                            }
                        }
                    }

                    if (count == 0)
                        return;

                    if (lastEtag <= lastMappedEtag)
                        return;

                    if (Log.IsDebugEnabled)
                        Log.Debug($"Executing map for '{Name} ({IndexId})'. Processed {count} documents in '{collection}' collection.");

                    WriteLastMappedEtag(indexContext.Transaction, collection, lastEtag);
                }

                _mre.Set(); // might be more
            }
        }
    }
}