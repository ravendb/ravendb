using System;
using System.Diagnostics;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Indexes.MapReduce.Static;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Documents;
using Raven.Server.Exceptions;
using Raven.Server.Indexing;
using Sparrow.Json;
using Voron.Impl;

namespace Raven.Server.Documents.Indexes.MapReduce.OutputToCollection
{
    public class OutputReduceIndexWriteOperation : IndexWriteOperation
    {
        private readonly OutputReduceToCollectionCommand _outputReduceToCollectionCommand;

        public OutputReduceIndexWriteOperation(MapReduceIndex index, LuceneVoronDirectory directory, LuceneDocumentConverterBase converter, Transaction writeTransaction,
            LuceneIndexPersistence persistence, JsonOperationContext indexContext)
            : base(index, directory, converter, writeTransaction, persistence)
        {
            Debug.Assert(index.OutputReduceToCollection != null);
            _outputReduceToCollectionCommand = index.OutputReduceToCollection.CreateCommand(indexContext, writeTransaction);
        }

        public override void Commit(IndexingStatsScope stats)
        {
            _outputReduceToCollectionCommand.SetIndexingStatsScope(stats);

            var enqueue = DocumentDatabase.TxMerger.Enqueue(_outputReduceToCollectionCommand);
            base.Commit(stats);
            try
            {
                using (stats.For(IndexingOperation.Reduce.SaveOutputDocuments))
                {
                    enqueue.GetAwaiter().GetResult();
                }
            }
            catch (Exception e)
            {
                throw new IndexWriteException("Failed to save output reduce documents to disk", e);
            }
        }

        public override void IndexDocument(LazyStringValue key, object document, IndexingStatsScope stats, JsonOperationContext indexContext)
        {
            base.IndexDocument(key, document, stats, indexContext);

            _outputReduceToCollectionCommand.AddReduce(key, document);
        }

        public override void Delete(LazyStringValue key, IndexingStatsScope stats)
        {
            throw new NotSupportedException("Deleting index entries by id() field isn't supported by map-reduce indexes");
        }

        public override void DeleteReduceResult(LazyStringValue reduceKeyHash, IndexingStatsScope stats)
        {
            base.DeleteReduceResult(reduceKeyHash, stats);

            _outputReduceToCollectionCommand.DeleteReduce(reduceKeyHash);
        }

        public override void Dispose()
        {
            base.Dispose();

            _outputReduceToCollectionCommand.Dispose();
        }
    }
}
