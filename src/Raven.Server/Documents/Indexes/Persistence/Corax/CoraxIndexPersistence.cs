using System;
using Corax;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.Indexing;
using Sparrow.Json;
using Sparrow.Logging;
using Sparrow.Threading;
using Voron;
using Voron.Impl;

namespace Raven.Server.Documents.Indexes.Persistence.Corax
{
    public class CoraxIndexPersistence : IndexPersistenceBase
    {
        private readonly Logger _logger;
        private readonly CoraxDocumentConverter _converter;

        public CoraxIndexPersistence(Index index) : base(index)
        {
            _logger = LoggingSource.Instance.GetLogger<CoraxIndexPersistence>(index.DocumentDatabase.Name);
            _converter = new CoraxDocumentConverter(index);
        }

        

        public override IndexWriteOperationBase OpenIndexWriter(Transaction writeTransaction, JsonOperationContext indexContext)
        {
            return new CoraxIndexWriteOperation(
                _index,
                writeTransaction,
                _converter, 
                _logger
            );
        }

        public override IndexReadOperationBase OpenIndexReader(Transaction readTransaction) => new CoraxIndexReadOperation(_index, _logger, readTransaction);

        public override bool ContainsField(string field)
        {
            if (field == Constants.Documents.Indexing.Fields.DocumentIdFieldName)
                return _index.Type.IsMap();

            return _index.Definition.IndexFields.ContainsKey(field);
        }

        public override IndexFacetedReadOperation OpenFacetedIndexReader(Transaction readTransaction)
        {
            throw new NotImplementedException();
        }

        public override SuggestionIndexReaderBase OpenSuggestionIndexReader(Transaction readTransaction, string field)
        {
            throw new NotImplementedException();
        }

        public override void Dispose()
        {
            _converter?.Dispose();
        }

        #region LuceneMethods

        internal override LuceneVoronDirectory LuceneDirectory { get; }
        public override bool HasWriter { get; }

        public override void CleanWritersIfNeeded()
        {
            // lucene method
        }

        public override void Clean(IndexCleanup mode)
        {
            // lucene method
        }

        public override void Initialize(StorageEnvironment environment)
        {
            // lucene method
        }

        public override void PublishIndexCacheToNewTransactions(IndexTransactionCache transactionCache)
        {
            _streamsCache = transactionCache;
        }

        internal override IndexTransactionCache BuildStreamCacheAfterTx(Transaction tx)
        {
            //lucene method

            return null;
        }

        internal override void RecreateSearcher(Transaction asOfTx)
        {
            //lucene method
        }

        internal override void RecreateSuggestionsSearchers(Transaction asOfTx)
        {
            //lucene method
        }

        public override void DisposeWriters()
        {
            //lucene method
        }
        #endregion
    }
}
