using System;
using System.Diagnostics;
using System.Threading.Tasks;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Indexes.MapReduce.Static;
using Raven.Server.Documents.Indexes.Persistence;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Documents;
using Raven.Server.Exceptions;
using Raven.Server.Indexing;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Server.Exceptions;
using Voron.Impl;

namespace Raven.Server.Documents.Indexes.MapReduce.OutputToCollection
{
    public class OutputReduceLuceneIndexWriteOperation : LuceneIndexWriteOperation
    {
        private readonly OutputReduceIndexWriteOperationScope<OutputReduceLuceneIndexWriteOperation> _outputScope;

        public OutputReduceLuceneIndexWriteOperation(MapReduceIndex index, LuceneVoronDirectory directory, LuceneDocumentConverterBase converter, Transaction writeTransaction,
            LuceneIndexPersistence persistence, JsonOperationContext indexContext)
            : base(index, directory, converter, writeTransaction, persistence)
        {
            Debug.Assert(index.OutputReduceToCollection != null);
            _outputScope = new(index, writeTransaction, indexContext, this);
        }

        public override void Commit(IndexingStatsScope stats) => _outputScope.Commit(stats);

        public override void IndexDocument(LazyStringValue key, LazyStringValue sourceDocumentId, object document, IndexingStatsScope stats,
            JsonOperationContext indexContext) => _outputScope.IndexDocument(key, sourceDocumentId, document, stats, indexContext);

        public override void Delete(LazyStringValue key, IndexingStatsScope stats) => _outputScope.Delete(key, stats);

        public override void DeleteReduceResult(LazyStringValue reduceKeyHash, IndexingStatsScope stats) => _outputScope.DeleteReduceResult(reduceKeyHash, stats);

        public override void Dispose()
        {
            base.Dispose();
            _outputScope.Dispose();
        }
    }
}
