using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Threading;
using Raven.Client.Documents.Indexes;
using Raven.Client.Exceptions.Corax;
using Raven.Server.Documents.Indexes.MapReduce.Static;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.Documents.Indexes.Static.Counters;
using Raven.Server.Documents.Indexes.Static.TimeSeries;
using Raven.Server.Documents.Queries;
using Raven.Server.Logging;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Json;
using Sparrow.Logging;
using Sparrow.Server.Logging;
using Voron;
using Voron.Data;
using Voron.Data.CompactTrees;
using Voron.Data.Lookups;
using Voron.Data.Graphs;
using Voron.Impl;
using Constants = Raven.Client.Constants;
using IndexWriter = Corax.Indexing.IndexWriter;

namespace Raven.Server.Documents.Indexes.Persistence.Corax;

public sealed class CoraxIndexPersistence : IndexPersistenceBase
{
    private const bool DisableDictionaryTraining = false; // [DEBUG ONLY]: disable training.
    private readonly RavenLogger _logger;
    private readonly CoraxDocumentConverterBase _converter;

    internal readonly global::Corax.Querying.Planning.PlanCache SharedPlanCache;

    private const int MaxFieldsWithMultipleTermsToCache = 512;

    private static readonly ImmutableDictionary<Slice, HnswIndexCache> EmptyCaches = ImmutableDictionary.Create<Slice, HnswIndexCache>(SliceComparer.Instance);
    private ImmutableDictionary<Slice, HnswIndexCache> _hnswCaches;

    // Snapshot of the fields that hold more than one term, keyed by field name. Refreshed on commit by
    // UpdateIndexCache and read by CoraxIndexReadOperation to drive plan selection. The set only ever grows,
    // so a Volatile read/write of the latest snapshot is enough - a reader that sees a slightly newer snapshot
    // just picks a more conservative plan, which is always correct.
    private HashSet<string> _fieldsWithMultipleTerms;

    // Read by the sort elision. Two properties make it safe to read stale: it is published before the plan cache
    // generation is touched, and it only ever grows. So a reader taking the generation first and this second can
    // never see fewer numeric fields than its own transaction holds, and over-reporting only costs an elision.
    // Never null once an index is open - a null sends the searcher back to probing its transaction.
    private HashSet<string> _fieldsWithNumericTerms;

    private long _fieldsWithNumericTermsSignature;

    internal IndexWriter ActiveWriter;
    internal Dictionary<Slice, HashSet<long>> PendingDirtyVectorSets;

    public CoraxIndexPersistence(Index index, IIndexReadOperationFactory indexReadOperationFactory) : base(index, indexReadOperationFactory)
    {
        _logger = RavenLogManager.Instance.GetLoggerForIndex<CoraxIndexPersistence>(index);
        _converter = CreateConverter(index);
        SharedPlanCache = new global::Corax.Querying.Planning.PlanCache(
            index.Configuration.CoraxMaxPlansPerQuery,
            index.Configuration.CoraxMaxDistinctQueryPlans);
    }

    private int GetMaxNodesForVectorCache()
    {
        var cacheSizeBytes = _index.Configuration.CoraxVectorSearchCacheSize.GetValue(SizeUnit.Bytes);
        var bytesPerNode = HnswIndexCache.EstimateBytesPerNode(_index.Configuration.CoraxVectorDefaultNumberOfEdges);
        return (int)Math.Min(cacheSizeBytes / bytesPerNode, int.MaxValue);
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
        if (_hnswCaches != null)
        {
            foreach (var kv in _hnswCaches)
                kv.Value.Dispose();
            _hnswCaches = null;
    }
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
        using (var tx = environment.WriteTransaction())
        {
            // Warm the per-field HNSW node caches and the fields-with-multiple-terms snapshot, then publish
            // them on the transaction client state (vector caches) / persistence instance (fields snapshot).
            WarmInitialCaches(tx);
            tx.LowLevelTransaction.UpdateClientState(UpdateIndexCache(tx));

            // Modify a page so the transaction commits and the client state becomes visible to new transactions.
            tx.LowLevelTransaction.ModifyPage(0);

            tx.Commit();
        }
    }

    public override IndexStateRecord UpdateIndexCache(Transaction tx)
    {
        if (tx.LowLevelTransaction.TryGetClientState(out IndexStateRecord rec) is false)
            rec = IndexStateRecord.CreateEmpty();

        // Refresh the multi-valued field snapshot (rebuilt only if the tree actually grew) and publish it on
        // the persistence instance. If it changed, touch the plan cache so plans that depend on it recompute.
        var previous = Volatile.Read(ref _fieldsWithMultipleTerms);
        var fields = ReadFieldsWithMultipleTerms(tx, previous);
        if (fields != previous)
        {
            Volatile.Write(ref _fieldsWithMultipleTerms, fields);
            SharedPlanCache.TouchGeneration();
        }

        // Sort elision (RavenDB-27192). The warm plan memo is served without recomputing the structural key, so
        // the generation is the only thing that can invalidate it. The signature is the allocation-free detector;
        // a fresh index has no fields tree and folds to 0, the initial value, hence the null test.
        var numericSignature = ReadNumericTermsSignature(tx);
        var publishedNumericTerms = Volatile.Read(ref _fieldsWithNumericTerms);
        if (numericSignature != Volatile.Read(ref _fieldsWithNumericTermsSignature) || publishedNumericTerms is null)
        {
            // Sticky: a -L/-D lookup empties when its last posting list does, and a shrunk set would elide for
            // a reader whose transaction still holds those numbers.
            var fieldsWithNumericTerms = ReadFieldsWithNumericTerms(tx);
            if (publishedNumericTerms != null)
                fieldsWithNumericTerms.UnionWith(publishedNumericTerms);

            Volatile.Write(ref _fieldsWithNumericTermsSignature, numericSignature);

            // The signature tracks the raw tree, so it also moves on a shrink the union absorbs. Equal counts
            // mean equal content here, and re-planning every query for that is waste. Set first, generation second.
            if (publishedNumericTerms is null || fieldsWithNumericTerms.Count != publishedNumericTerms.Count)
            {
                Volatile.Write(ref _fieldsWithNumericTerms, fieldsWithNumericTerms);
                SharedPlanCache.TouchGeneration();
            }
        }

        var caches = Volatile.Read(ref _hnswCaches);
        return rec with { CoraxVectorState = caches is null ? CoraxVectorState.Empty : new CoraxVectorState(caches) };
    }

    // Walks the fields tree, not the index definition: a dynamic field is absent from the definition, and the -L/-D
    // lookups are created empty on a field's first write, so nothing but their own entry count moves at the first number.
    private static long ReadNumericTermsSignature(Transaction tx)
    {
        var fieldsTree = tx.ReadTree(global::Corax.Constants.IndexWriter.FieldsSlice);
        if (fieldsTree == null)
            return 0;

        // Seeded 1 so the key count enters the value: under a 0 seed this is a base-31 numeral and keys sorting
        // ahead of every numeric one are free leading zeros, so adding one while the bits shift reproduces the
        // old value. Still a 64-bit hash - a same-length cancellation is not defended against.
        long signature = 1;
        using var it = fieldsTree.Iterate(prefetch: false);
        if (it.Seek(Slices.BeforeAllKeys) == false)
            return signature;

        do
        {
            signature = signature * 31 + (HasNumericTerms(it) ? 1 : 0);
        } while (it.MoveNext());

        return signature;
    }

    // Base field names, so the searcher can match directly.
    private static HashSet<string> ReadFieldsWithNumericTerms(Transaction tx)
    {
        var fieldsTree = tx.ReadTree(global::Corax.Constants.IndexWriter.FieldsSlice);
        if (fieldsTree == null)
            return [];

        var set = new HashSet<string>(StringComparer.Ordinal);
        using var it = fieldsTree.Iterate(prefetch: false);
        if (it.Seek(Slices.BeforeAllKeys) == false)
            return set;

        do
        {
            if (HasNumericTerms(it) == false)
                continue;

            // -L and -D are the same length.
            var name = it.CurrentKey.ToString();
            set.Add(name[..^global::Corax.Constants.IndexWriter.LongTreeSuffix.Length]);
        } while (it.MoveNext());

        return set;
    }

    // The entry count is already in the root header under the cursor; opening the lookup would allocate per numeric
    // field per commit. RootObjectType is not optional: a field named Vitamin-D stores a CompactTree under that key.
    private static unsafe bool HasNumericTerms(IIterator it)
    {
        var key = it.CurrentKey;
        if (key.EndsWith(global::Corax.Constants.IndexWriter.LongTreeSuffix) == false &&
            key.EndsWith(global::Corax.Constants.IndexWriter.DoubleTreeSuffix) == false)
            return false;

        var state = (LookupState*)it.CreateReaderForCurrent().Base;
        return state->RootObjectType == RootObjectType.Lookup && state->NumberOfEntries > 0;
    }

    internal HashSet<string> FieldsWithNumericTerms => Volatile.Read(ref _fieldsWithNumericTerms);

    // Snapshot of the caller's current fields-with-multiple-terms; read by CoraxIndexReadOperation at reader open.
    internal HashSet<string> FieldsWithMultipleTerms => Volatile.Read(ref _fieldsWithMultipleTerms);

    // Cache MultipleTermsInField tree into a set. Write side only ever adds, so count tells when it changed.
    private static HashSet<string> ReadFieldsWithMultipleTerms(Transaction tx, HashSet<string> previous)
    {
        var tree = tx.ReadTree(global::Corax.Constants.IndexWriter.MultipleTermsInField);
        long count = tree != null ? tree.ReadHeader().NumberOfEntries : 0;
        if (count == 0)
            return null;

        // Unchanged since last build: the previous snapshot (or previous null, when over the cap) still holds.
        if (previous != null && previous.Count == count)
            return previous;
        if (count > MaxFieldsWithMultipleTermsToCache)
            return null;

        var set = new HashSet<string>((int)count, StringComparer.Ordinal);
        using (var it = tree!.Iterate(prefetch: false))
        {
            if (it.Seek(Slices.BeforeAllKeys))
            {
                do
                {
                    set.Add(it.CurrentKey.ToString());
                } while (it.MoveNext());
            }
        }

        return set;
    }

    internal override void RecreateSearcher(Transaction asOfTx)
    {
        var dirty = PendingDirtyVectorSets;
        PendingDirtyVectorSets = null;
        if (dirty == null)
            return;

        var maxNodes = GetMaxNodesForVectorCache();
        if (maxNodes <= 0)
        {
            // Cache turned off at runtime (CoraxVectorSearchCacheSize == 0): clear the caches. UpdateIndexCache
            // publishes the cleared state on commit, so new read transactions resolve vectors from disk.
            // In-flight readers keep their captured snapshot; the dropped instances release their native
            // memory via finalization once those transactions complete.
            Volatile.Write(ref _hnswCaches, null);
            return;
        }

        var llt = asOfTx.LowLevelTransaction;
        var current = _hnswCaches;
        ImmutableDictionary<Slice, HnswIndexCache>.Builder freshlyAdded = null;
        foreach (var kv in dirty)
        {
            if (current != null && current.TryGetValue(kv.Key, out var cache))
            {
                cache.ApplyCommit(llt, kv.Key, kv.Value);
                continue;
            }

            var fresh = HnswIndexCache.WarmFromScratch(llt, kv.Key, maxNodes);
            if (fresh is null)
                continue;
            (freshlyAdded ??= ImmutableDictionary.CreateBuilder<Slice, HnswIndexCache>(SliceComparer.Instance))[kv.Key] = fresh;
        }

        if (freshlyAdded is null)
            return;

        // Build a new dictionary that adds the freshly-warmed caches while sharing structure with the
        // current one, then swap it in. Readers holding the old dictionary keep using it unchanged.
        var grown = (current ?? EmptyCaches).SetItems(freshlyAdded);

        Volatile.Write(ref _hnswCaches, grown);
    }

    private void WarmInitialCaches(Transaction tx)
    {
        var maxNodes = GetMaxNodesForVectorCache();
        if (maxNodes <= 0)
            return;

        // Vector fields are read from the index definition, not the fields mapping: field discovery during
        // initialization must stay independent of analyzer construction.
        var vectorFieldNames = _converter?.GetVectorFieldNames();
        if (vectorFieldNames is null)
            return;

        var llt = tx.LowLevelTransaction;
        ImmutableDictionary<Slice, HnswIndexCache>.Builder builder = null;
        foreach (var fieldName in vectorFieldNames)
        {
            Debug.Assert(fieldName is { HasValue: true, Size: > 0 },
                "Vector field name must be allocated and non-empty for cache keying");
            var cache = HnswIndexCache.WarmFromScratch(llt, fieldName, maxNodes);
            if (cache is null)
                continue;
            builder ??= ImmutableDictionary.CreateBuilder<Slice, HnswIndexCache>(SliceComparer.Instance);
            builder[fieldName] = cache;
        }

        if (builder != null)
            _hnswCaches = builder.ToImmutable();
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
