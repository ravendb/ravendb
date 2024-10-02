using System;
using System.Threading;
using Raven.Client.Documents.Indexes;
using Raven.Client.Exceptions.Corax;
using Raven.Server.Documents.Indexes.MapReduce.Static;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.Documents.Indexes.Static.Counters;
using Raven.Server.Documents.Indexes.Static.TimeSeries;
using Raven.Server.Documents.Queries;
using Raven.Server.Indexing;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Logging;
using Voron;
using Voron.Data.CompactTrees;
using Voron.Impl;
using Constants = Raven.Client.Constants;

namespace Raven.Server.Documents.Indexes.Persistence.Corax;

public sealed class CoraxIndexPersistence : IndexPersistenceBase
{
    private const bool DisableDictionaryTraining = false; // [DEBUG ONLY]: disable training.
    private readonly Logger _logger;
    private readonly CoraxDocumentConverterBase _converter;
    public CoraxIndexPersistence(Index index, IIndexReadOperationFactory indexReadOperationFactory) : base(index, indexReadOperationFactory)
    {
        _logger = LoggingSource.Instance.GetLogger<CoraxIndexPersistence>(index.DocumentDatabase.Name);
        _converter = CreateConverter(index);
    }

    private CoraxDocumentConverterBase CreateConverter(Index index)
    {
        bool storeValue = false;
        switch (index.Type)
        {
            case IndexType.AutoMapReduce:
                storeValue = true;
                break;
            case IndexType.MapReduce:
                return new AnonymousCoraxDocumentConverter(index, true);
            case IndexType.Map:
                switch (_index.SourceType)
                {
                    case IndexSourceType.Documents:
                        return new AnonymousCoraxDocumentConverter(index);
                    case IndexSourceType.TimeSeries:
                    case IndexSourceType.Counters:
                        return new CountersAndTimeSeriesAnonymousCoraxDocumentConverter(index);
                }
                break;
            case IndexType.JavaScriptMap:
                switch (_index.SourceType)
                {
                    case IndexSourceType.Documents:
                        return new CoraxJintDocumentConverter((MapIndex)index);
                    case IndexSourceType.TimeSeries:
                        return new CountersAndTimeSeriesJintCoraxDocumentConverter((MapTimeSeriesIndex)index);
                    case IndexSourceType.Counters:
                        return new CountersAndTimeSeriesJintCoraxDocumentConverter((MapCountersIndex)index);
                }
                break;
            case IndexType.JavaScriptMapReduce:
                return new CoraxJintDocumentConverter((MapReduceIndex)index, storeValue: true);
        }

        return new CoraxDocumentConverter(index, storeValue: storeValue);
    }

    public override IndexReadOperationBase OpenIndexReader(Transaction readTransaction, IndexQueryServerSide query = null)
    {
        return IndexReadOperationFactory.CreateCoraxIndexReadOperation(_index, _logger, readTransaction, _index._queryBuilderFactories,
            _converter.GetKnownFieldsForQuerying(), query);
    }

    public override bool ContainsField(string field)
    {
        if (field == Constants.Documents.Indexing.Fields.DocumentIdFieldName)
            return _index.Type.IsMap();

        return _index.Definition.IndexFields.ContainsKey(field);
    }

    public override IndexFacetReadOperationBase OpenFacetedIndexReader(Transaction readTransaction)
    {
        return new CoraxIndexFacetedReadOperation(_index, _logger, readTransaction, _index._queryBuilderFactories, _converter.GetKnownFieldsForQuerying());
    }

    public override SuggestionIndexReaderBase OpenSuggestionIndexReader(Transaction readTransaction, string field)
    {
        if (_converter.GetKnownFieldsForQuerying().TryGetByFieldName(readTransaction.Allocator, field, out var binding) == false)
            throw new InvalidOperationException($"No suggestions index found for field '{field}'.");

        return new CoraxSuggestionReader(_index, _logger, binding, readTransaction, _converter.GetKnownFieldsForQuerying());
    }

    public override void Dispose()
    {
        _converter?.Dispose();
    }

    public override bool RequireOnBeforeExecuteIndexing()
    {
        var contextPool = _index._contextPool;
        using (contextPool.AllocateOperationContext(out TransactionOperationContext context))
        using (var tx = context.OpenReadTransaction())
        {
            if (CompactTree.HasDictionary(tx.InnerTransaction.LowLevelTransaction))
                return false; 
        }

        if (_index.IsTestRun)
            return false;
        
        if (_index.SourceType != IndexSourceType.Documents)
            return false;

        return true;
    }

    public override void OnBeforeExecuteIndexing(IndexingStatsAggregator indexingStatsAggregator, CancellationToken token)
    {
        CreatePersistentDictionary(indexingStatsAggregator, token);
    }

    private void CreatePersistentDictionary(IndexingStatsAggregator indexingStatsAggregator, CancellationToken token)
    {
        var contextPool = _index._contextPool;
        var documentStorage = _index.DocumentDatabase.DocumentsStorage;
        
        using var scope = indexingStatsAggregator.CreateScope();
        using var indexingStatsScope = scope.For(IndexingOperation.Corax.DictionaryTraining);
        using var __ = CultureHelper.EnsureInvariantCulture();
        using var ___ = contextPool.AllocateOperationContext(out TransactionOperationContext indexContext);
        using var queryContext = QueryOperationContext.Allocate(_index.DocumentDatabase, _index);
        using (CurrentIndexingScope.Current = _index.CreateIndexingScope(indexContext, queryContext))
        {
            indexContext.PersistentContext.LongLivedTransactions = true;
            queryContext.SetLongLivedTransactions(true);

            using var readTx = queryContext.OpenReadTransaction();
            using var tx = indexContext.OpenWriteTransaction();
            
            // We are creating a new converter because converters get tied through their accessors to the structure, and since on Map-Reduce indexes
            // we only care about the map and not the reduce hilarity can ensure when properties do not share the type. 
            var converter = CreateConverter(_index);
            converter.IgnoreComplexObjectsDuringIndex = true; // for training, we don't care
            
            var enumerator = new CoraxDocumentTrainEnumerator(indexContext, converter, _index, _index.Type, documentStorage, queryContext.Documents, _index.Collections, token, indexingStatsScope, _index.Configuration.DocumentsLimitForCompressionDictionaryCreation);

            var llt = tx.InnerTransaction.LowLevelTransaction;

            if (DisableDictionaryTraining || PersistentDictionary.TryCreate(llt, enumerator, out var _) == false)
                PersistentDictionary.CreateDefault(llt);

            tx.Commit();
        }
    }
    
    #region LuceneMethods

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
    }
    
    public override void PublishIndexCacheToNewTransactions(IndexTransactionCache transactionCache)
    {
        //lucene method
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
    
    public override IndexWriteOperationBase OpenIndexWriter(Transaction writeTransaction, JsonOperationContext indexContext)
    {
        if (_index.Type == IndexType.MapReduce || _index.Type == IndexType.JavaScriptMapReduce)
        {
            var mapReduceIndex = (MapReduceIndex)_index;
            if (string.IsNullOrWhiteSpace(mapReduceIndex.Definition.OutputReduceToCollection) == false)
                return new OutputReduceCoraxIndexWriteOperation(mapReduceIndex, writeTransaction, _converter, _logger, indexContext);
        }
        
        return new CoraxIndexWriteOperation(
            _index,
            writeTransaction,
            _converter,
            _logger
        );
    }

    public override void AssertCanOptimize()
    {
        throw new NotSupportedInCoraxException("Optimize is not supported in Corax.");
    }

    public override void AssertCanDump()
    {
        throw new NotSupportedInCoraxException("Dump is not supported in Corax.");
    }
}
