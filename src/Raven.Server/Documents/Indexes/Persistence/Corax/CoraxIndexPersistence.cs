using System;
using System.Collections.Generic;
using Lucene.Net.Store;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.Indexing;
using Sparrow.Json;
using Voron;
using Voron.Impl;

namespace Raven.Server.Documents.Indexes.Persistence.Corax
{
    public class CoraxIndexPersistence : IndexPersistenceBase
    {
        public CoraxIndexPersistence(Index index) : base(index)
        {
        }

        internal override LuceneVoronDirectory LuceneDirectory { get; }
        public override void CleanWritersIfNeeded()
        {
            throw new NotImplementedException();
        }

        public override void Clean(IndexCleanup mode)
        {
            throw new NotImplementedException();
        }

        public override void Initialize(StorageEnvironment environment)
        {
            throw new NotImplementedException();
        }

        public override void PublishIndexCacheToNewTransactions(IndexTransactionCache transactionCache)
        {
            throw new NotImplementedException();
        }

        internal override IndexTransactionCache BuildStreamCacheAfterTx(Transaction tx)
        {
            throw new NotImplementedException();
        }

        public override IndexWriteOperationBase OpenIndexWriter(Transaction writeTransaction, JsonOperationContext indexContext)
        {
            throw new NotImplementedException();
        }

        public override IndexReadOperationBase OpenIndexReader(Transaction readTransaction)
        {
            throw new NotImplementedException();
        }

        public override bool ContainsField(string field)
        {
            throw new NotImplementedException();
        }

        public override IndexFacetedReadOperation OpenFacetedIndexReader(Transaction readTransaction)
        {
            throw new NotImplementedException();
        }

        public override SuggestionIndexReaderBase OpenSuggestionIndexReader(Transaction readTransaction, string field)
        {
            throw new NotImplementedException();
        }

        internal override void RecreateSearcher(Transaction asOfTx)
        {
            throw new NotImplementedException();
        }

        internal override void RecreateSuggestionsSearchers(Transaction asOfTx)
        {
            throw new NotImplementedException();
        }

        public override void DisposeWriters()
        {
            throw new NotImplementedException();
        }

        public override void Dispose()
        {
            throw new NotImplementedException();
        }
    }
}
