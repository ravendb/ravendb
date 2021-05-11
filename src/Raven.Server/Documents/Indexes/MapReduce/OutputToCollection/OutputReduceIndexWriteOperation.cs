using System;
using System.Diagnostics;
using System.Threading.Tasks;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Indexes.MapReduce.Static;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Documents;
using Raven.Server.Exceptions;
using Raven.Server.Indexing;
using Raven.Server.Utils;
using Sparrow.Json;
using Voron.Impl;

namespace Raven.Server.Documents.Indexes.MapReduce.OutputToCollection
{
    public class OutputReduceIndexWriteOperation : IndexWriteOperation
    {
        private readonly OutputReduceToCollectionCommandBatcher _outputReduceToCollectionCommandBatcher;
        private readonly TransactionHolder _txHolder;

        public OutputReduceIndexWriteOperation(MapReduceIndex index, LuceneVoronDirectory directory, LuceneDocumentConverterBase converter, Transaction writeTransaction,
            LuceneIndexPersistence persistence, JsonOperationContext indexContext)
            : base(index, directory, converter, writeTransaction, persistence)
        {
            Debug.Assert(index.OutputReduceToCollection != null);
            _txHolder = new TransactionHolder(writeTransaction);
            _outputReduceToCollectionCommandBatcher = index.OutputReduceToCollection.CreateCommandBatcher(indexContext, _txHolder);
        }

        public override void Commit(IndexingStatsScope stats)
        {
            var enqueue = CommitOutputReduceToCollection();

            using (_txHolder.AcquireTransaction(out _))
            {
                base.Commit(stats);
            }

            try
            {
                using (stats.For(IndexingOperation.Reduce.SaveOutputDocuments))
                {
                    enqueue.GetAwaiter().GetResult();
                }
            }
            catch (TaskCanceledException)
            {
                throw;
            }
            catch (ObjectDisposedException e) when (DocumentDatabase.DatabaseShutdown.IsCancellationRequested)
            {
                throw new TaskCanceledException("The operation of writing output reduce documents was cancelled because of database shutdown", e);
            }
            catch (Exception e)
            {
                throw new IndexWriteException("Failed to save output reduce documents to disk", e);
            }
        }

        private async Task CommitOutputReduceToCollection()
        {
            foreach (var command in _outputReduceToCollectionCommandBatcher.CreateCommands())
                await DocumentDatabase.TxMerger.Enqueue(command).ConfigureAwait(false);
        }

        public override void IndexDocument(LazyStringValue key, LazyStringValue sourceDocumentId, object document, IndexingStatsScope stats, JsonOperationContext indexContext)
        {
            base.IndexDocument(key, sourceDocumentId, document, stats, indexContext);

            _outputReduceToCollectionCommandBatcher.AddReduce(key, document, stats);
        }

        public override void Delete(LazyStringValue key, IndexingStatsScope stats)
        {
            throw new NotSupportedException("Deleting index entries by id() field isn't supported by map-reduce indexes");
        }

        public override void DeleteReduceResult(LazyStringValue reduceKeyHash, IndexingStatsScope stats)
        {
            base.DeleteReduceResult(reduceKeyHash, stats);

            _outputReduceToCollectionCommandBatcher.DeleteReduce(reduceKeyHash);
        }

        public override void Dispose()
        {
            base.Dispose();

            _outputReduceToCollectionCommandBatcher.Dispose();
        }
    }
}
