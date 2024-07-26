using System;
using System.Threading;
using Lucene.Net.Search;
using Lucene.Net.Store;
using Raven.Server.Documents.Queries;
using Raven.Server.Indexing;
using Sparrow.Json;
using Voron;
using Voron.Impl;

namespace Raven.Server.Documents.Indexes.Persistence
{
    public abstract class IndexPersistenceBase : IDisposable
    {
        protected readonly Index _index;

        protected IndexPersistenceBase(Index index, IIndexReadOperationFactory indexReadOperationFactory)
        {
            IndexReadOperationFactory = indexReadOperationFactory;
            _index = index;
        }

        protected IIndexReadOperationFactory IndexReadOperationFactory { get; }

        public abstract bool HasWriter { get; }
        public abstract void CleanWritersIfNeeded();

        public abstract void Clean(IndexCleanup mode);

        public abstract void Initialize(StorageEnvironment environment);

        public abstract bool RequireOnBeforeExecuteIndexing();
        
        public abstract void OnBeforeExecuteIndexing(IndexingStatsAggregator indexingStatsAggregator, CancellationToken token);

        public abstract IndexWriteOperationBase OpenIndexWriter(Transaction writeTransaction, JsonOperationContext indexContext);

        public abstract IndexReadOperationBase OpenIndexReader(Transaction readTransaction, IndexQueryServerSide query = null);

        public abstract bool ContainsField(string field);
        public abstract IndexFacetReadOperationBase OpenFacetedIndexReader(Transaction readTransaction);
        public abstract SuggestionIndexReaderBase OpenSuggestionIndexReader(Transaction readTransaction, string field);
        public abstract void AssertCanOptimize();
        public abstract void AssertCanDump();
        internal abstract void RecreateSearcher(Transaction asOfTx);
        internal abstract void RecreateSuggestionsSearchers(Transaction asOfTx);
        public abstract void DisposeWriters();
        public abstract void Dispose();

        public virtual IndexStateRecord UpdateIndexCache(Transaction tx)
        {
            return tx.LowLevelTransaction.TryGetClientState(out IndexStateRecord record) ? record : null;
        }

    }
}
