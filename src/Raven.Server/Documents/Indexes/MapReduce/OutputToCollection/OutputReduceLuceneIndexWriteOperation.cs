using System.Diagnostics;
using Raven.Server.Documents.Indexes.MapReduce.Static;
using Raven.Server.Documents.Indexes.Persistence;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Documents;
using Raven.Server.Indexing;
using Sparrow.Json;
using Voron.Impl;

namespace Raven.Server.Documents.Indexes.MapReduce.OutputToCollection
{
    public sealed class OutputReduceLuceneIndexWriteOperation : LuceneIndexWriteOperation
    {
        private readonly OutputReduceIndexWriteOperationScope<OutputReduceLuceneIndexWriteOperation> _outputScope;

        public OutputReduceLuceneIndexWriteOperation(MapReduceIndex index, LuceneVoronDirectory directory, LuceneDocumentConverterBase converter, Transaction writeTransaction,
            LuceneIndexPersistence persistence, JsonOperationContext indexContext)
            : base(index, directory, converter, writeTransaction, persistence)
        {
            Debug.Assert(index.OutputReduceToCollection != null);
            _outputScope = new(index, writeTransaction, indexContext, this);
        }

        public override void Commit(IndexingStatsScope stats)
        {
            if (_outputScope.IsActive)
                base.Commit(stats);
            else
                _outputScope.Commit(stats);
        }

        public override void IndexDocument(LazyStringValue key, LazyStringValue sourceDocumentId, object document, IndexingStatsScope stats,
            JsonOperationContext indexContext)
        {
            if (_outputScope.IsActive)
                base.IndexDocument(key, sourceDocumentId, document, stats, indexContext);
            else
                _outputScope.IndexDocument(key, sourceDocumentId, document, stats, indexContext);
        }

        public override void Delete(LazyStringValue key, IndexingStatsScope stats)
        {
            if (_outputScope.IsActive)
                base.Delete(key, stats);
            else
                _outputScope.Delete(key, stats);
        }

        public override void DeleteTimeSeries(LazyStringValue docId, LazyStringValue key, IndexingStatsScope stats)
        {
            if (_outputScope.IsActive)
                base.DeleteTimeSeries(docId, key, stats);
            else
                _outputScope.Delete(key, stats);
        }

        public override void DeleteReduceResult(LazyStringValue reduceKeyHash, IndexingStatsScope stats)
        {
            if (_outputScope.IsActive)
                base.DeleteReduceResult(reduceKeyHash, stats);
            else
                _outputScope.DeleteReduceResult(reduceKeyHash, stats);
        }

        public override void Dispose()
        {
            base.Dispose();
            _outputScope.Dispose();
        }
    }
}
