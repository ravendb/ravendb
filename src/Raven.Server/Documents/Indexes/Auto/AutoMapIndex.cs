using System;
using System.Diagnostics;
using System.Threading;

using Raven.Abstractions.Data;
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

        public override void DoIndexingWork(CancellationToken cancellationToken)
        {
            DocumentsOperationContext databaseContext;
            TransactionOperationContext indexContext;

            using (DocumentDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out databaseContext))
            using (_contextPool.AllocateOperationContext(out indexContext))
            using (var tx = indexContext.OpenWriteTransaction())
            {
                ExecuteCleanup(cancellationToken, databaseContext, indexContext);
                ExecuteMap(cancellationToken, databaseContext, indexContext);

                tx.Commit();
            }
        }

        private void ExecuteCleanup(CancellationToken token, DocumentsOperationContext databaseContext, TransactionOperationContext indexContext)
        {
            var pageSize = DocumentDatabase.Configuration.Indexing.MaxNumberOfTombstonesToFetch;

            foreach (var collection in Collections)
            {
                long lastMappedEtag;
                long lastTombstoneEtag;
                lastMappedEtag = ReadLastMappedEtag(indexContext.Transaction, collection);
                lastTombstoneEtag = ReadLastTombstoneEtag(indexContext.Transaction, collection);

                var lastEtag = lastTombstoneEtag;
                var count = 0;

                using (var indexActions = IndexPersistence.OpenIndexWriter(indexContext.Transaction.InnerTransaction))
                {
                    using (databaseContext.OpenReadTransaction())
                    {
                        var sw = Stopwatch.StartNew();
                        foreach (
                            var tombstone in
                                DocumentDatabase.DocumentsStorage.GetTombstonesAfter(databaseContext, collection,
                                    lastEtag + 1, 0, pageSize))
                        {
                            token.ThrowIfCancellationRequested();

                            count++;
                            lastEtag = tombstone.Etag;

                            if (tombstone.DeletedEtag > lastMappedEtag)
                                continue; // no-op, we have not yet indexed this document

                            indexActions.Delete(tombstone.Key);

                            if (sw.Elapsed >
                                DocumentDatabase.Configuration.Indexing.TombstoneProcessingTimeout.AsTimeSpan)
                            {
                                break;
                            }
                        }
                    }
                }

                if (count == 0)
                    return;

                if (lastEtag <= lastTombstoneEtag)
                    return;

                WriteLastTombstoneEtag(indexContext.Transaction, collection, lastEtag);


                _mre.Set(); // might be more
            }
        }


        private void ExecuteMap(CancellationToken cancellationToken, DocumentsOperationContext databaseContext, TransactionOperationContext indexContext)
        {
            var pageSize = DocumentDatabase.Configuration.Indexing.MaxNumberOfDocumentsToFetchForMap;

            foreach (var collection in Collections)
            {
                long lastMappedEtag;
                lastMappedEtag = ReadLastMappedEtag(indexContext.Transaction, collection);

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

                            count++;
                            lastEtag = document.Etag;

                            try
                            {
                                indexWriter.IndexDocument(document);
                                DocumentDatabase.Metrics.IndexedPerSecond.Mark();
                            }
                            catch (Exception e)
                            {
                                // TODO [ppekrol] log?
                                Console.WriteLine(e);
                                throw;
                            }

                            if (sw.Elapsed >
                                DocumentDatabase.Configuration.Indexing.DocumentProcessingTimeout.AsTimeSpan)
                            {
                                break;
                            }
                        }
                    }

                    if (count == 0)
                        return;

                    if (lastEtag <= lastMappedEtag)
                        return;

                    WriteLastMappedEtag(indexContext.Transaction, collection, lastEtag);
                }

                _mre.Set(); // might be more
            }
        }
    }
}