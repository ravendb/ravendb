using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Raven.Client.Data.Indexes;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.Results;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Voron;
using Voron.Data.BTrees;
using Voron.Data.Fixed;
using Voron.Impl;
using Sparrow;
using Voron.Util;

namespace Raven.Server.Documents.Indexes.MapReduce
{
    public abstract class MapReduceIndexBase<T> : Index<T> where T : IndexDefinitionBase
    {
        internal const string MapEntriesTreeName = "MapEntries";
        internal const string ResultsStoreTypesTreeName = "ResultsStoreTypes";

        private PageLocator _pageLocator;
        internal readonly MapReduceIndexingContext MapReduceWorkContext = new MapReduceIndexingContext();

        private IndexingStatsScope _statsInstance;
        private MapPhaseStats _stats = new MapPhaseStats();

        protected MapReduceIndexBase(int indexId, IndexType type, T definition) : base(indexId, type, definition)
        {
        }

        public override IDisposable InitializeIndexingWork(TransactionOperationContext indexContext)
        {
            MapReduceWorkContext.MapEntries = GetMapEntriesTree(indexContext.Transaction.InnerTransaction);
            MapReduceWorkContext.ResultsStoreTypes = MapReduceWorkContext.MapEntries.FixedTreeFor(ResultsStoreTypesTreeName, sizeof(byte));

            _pageLocator = new PageLocator(indexContext.Transaction.InnerTransaction.LowLevelTransaction, 128);

            MapReduceWorkContext.DocumentMapEntries = new FixedSizeTree(
                   indexContext.Transaction.InnerTransaction.LowLevelTransaction,
                   MapReduceWorkContext.MapEntries,
                   Slices.Empty,
                   sizeof(ulong),
                   clone: false, 
                   pageLocator: _pageLocator);

            return MapReduceWorkContext;
        }

        public override unsafe void HandleDelete(DocumentTombstone tombstone, string collection, IndexWriteOperation writer,
            TransactionOperationContext indexContext, IndexingStatsScope stats)
        {
            Slice docKeyAsSlice;            
            using (Slice.External(indexContext.Allocator, tombstone.LoweredKey.Buffer, tombstone.LoweredKey.Length, out docKeyAsSlice))
            {
                MapReduceWorkContext.DocumentMapEntries.RepurposeInstance(docKeyAsSlice, clone: false);

                if (MapReduceWorkContext.DocumentMapEntries.NumberOfEntries == 0)
                    return;

                foreach (var mapEntry in GetMapEntries(MapReduceWorkContext.DocumentMapEntries))
                {
                    var store = GetResultsStore(mapEntry.ReduceKeyHash, indexContext, create: false, pageLocator: _pageLocator);

                    store.Delete(mapEntry.Id);
                }

                MapReduceWorkContext.MapEntries.DeleteFixedTreeFor(tombstone.LoweredKey, sizeof(ulong));
            }

            
        }

        public override IQueryResultRetriever GetQueryResultRetriever(DocumentsOperationContext documentsContext, TransactionOperationContext indexContext, FieldsToFetch fieldsToFetch)
        {
            return new MapReduceQueryResultRetriever(documentsContext, fieldsToFetch);
        }

        private static Tree GetMapEntriesTree(Transaction tx)
        {
            // map entries structure
            // MapEntries tree has the following entries
            // 1) { document key, fixed size tree }
            // each fixed size tree stores records like 
            //   |----> { identifier of a map result, hash of a reduce key for the map result }
            // 2) entry to keep track of the identifier of last stored entry { #LastMapResultId, long_value }
            // 3) { ResultsStoreTypes, fixed size tree } where the fixed size tree stores records like
            //   |----> { reduce key hash, MapResultsStorageType enum } 

            return tx.CreateTree(MapEntriesTreeName);
        }

        protected unsafe int PutMapResults(LazyStringValue documentKey, IEnumerable<MapResult> mappedResults, TransactionOperationContext indexContext, IndexingStatsScope stats)
        {
            EnsureValidStats(stats);

            Slice docKeyAsSlice;            
            using (Slice.External(indexContext.Allocator, documentKey.Buffer, documentKey.Length, out docKeyAsSlice))
            {
                Queue<MapEntry> existingEntries = null;

                using (_stats.GetMapEntriesTree.Start())
                    MapReduceWorkContext.DocumentMapEntries.RepurposeInstance(docKeyAsSlice, clone: false);


                if (MapReduceWorkContext.DocumentMapEntries.NumberOfEntries > 0)
                {
                    using (_stats.GetMapEntries.Start())
                        existingEntries = GetMapEntries(MapReduceWorkContext.DocumentMapEntries);
                }

                int resultsCount = 0;

                foreach (var mapResult in mappedResults)
                {
                    using (mapResult.Data)
                    {
                        resultsCount++;

                        var reduceKeyHash = mapResult.ReduceKeyHash;

                        long id = -1;

                        if (existingEntries?.Count > 0)
                        {
                            var existing = existingEntries.Dequeue();
                            var storeOfExisting = GetResultsStore(existing.ReduceKeyHash, indexContext, false, _pageLocator);

                            if (reduceKeyHash == existing.ReduceKeyHash)
                            {
                                var existingResult = storeOfExisting.Get(existing.Id);

                                if (ResultsBinaryEqual(mapResult.Data, existingResult))
                                {
                                    continue;
                                }

                                id = existing.Id;
                            }
                            else
                            {
                                using (_stats.RemoveResult.Start())
                                {
                                    MapReduceWorkContext.DocumentMapEntries.Delete(existing.Id);
                                    storeOfExisting.Delete(existing.Id);
                                }
                            }
                        }

                        using (_stats.PutResult.Start())
                        {
                            if (id == -1)
                            {
                                id = MapReduceWorkContext.GetNextIdentifier();

                                Slice val;
                                using (Slice.External(indexContext.Allocator, (byte*)&reduceKeyHash, sizeof(ulong), out val))
                                    MapReduceWorkContext.DocumentMapEntries.Add(id, val);
                            }

                            GetResultsStore(reduceKeyHash, indexContext, create: true, pageLocator: _pageLocator).Add(id, mapResult.Data);
                        }
                    }
                }

                DocumentDatabase.Metrics.MapReduceMappedPerSecond.Mark(resultsCount);

                while (existingEntries?.Count > 0)
                {
                    // need to remove remaining old entries

                    var oldResult = existingEntries.Dequeue();

                    var oldState = GetResultsStore(oldResult.ReduceKeyHash, indexContext, create: false, pageLocator: _pageLocator);

                    using (_stats.RemoveResult.Start())
                    {
                        oldState.Delete(oldResult.Id);
                        MapReduceWorkContext.DocumentMapEntries.Delete(oldResult.Id);
                    }
                }

                return resultsCount;
            }            
        }

        private static unsafe bool ResultsBinaryEqual(BlittableJsonReaderObject newResult, PtrSize existingData)
        {
            return newResult.Size == existingData.Size &&
                   Memory.CompareInline(newResult.BasePointer, existingData.Ptr, existingData.Size) == 0;
        }

        private static unsafe Queue<MapEntry> GetMapEntries(FixedSizeTree documentMapEntries)
        {
            var entries = new Queue<MapEntry>((int)documentMapEntries.NumberOfEntries);

            using (var it = documentMapEntries.Iterate())
            {
                do
                {
                    var currentKey = it.CurrentKey;
                    ulong reduceKeyHash;

                    it.CreateReaderForCurrent().Read((byte*)&reduceKeyHash, sizeof(ulong));

                    entries.Enqueue(new MapEntry
                    {
                        Id = currentKey,
                        ReduceKeyHash = reduceKeyHash
                    });
                } while (it.MoveNext());
            }

            return entries;
        }

        public unsafe MapReduceResultsStore GetResultsStore(ulong reduceKeyHash, TransactionOperationContext indexContext, bool create, PageLocator pageLocator = null)
        {
            MapReduceResultsStore store;
            if (MapReduceWorkContext.StoreByReduceKeyHash.TryGetValue(reduceKeyHash, out store) == false)
            {
                Slice read;
                using (MapReduceWorkContext.ResultsStoreTypes.Read((long)reduceKeyHash, out read))
                {
                    MapResultsStorageType type;

                    if (read.HasValue)
                        type = (MapResultsStorageType)(*read.CreateReader().Base);
                    else
                        type = MapResultsStorageType.Nested;

                    store = new MapReduceResultsStore(reduceKeyHash, type, indexContext, MapReduceWorkContext, create, pageLocator);

                    MapReduceWorkContext.StoreByReduceKeyHash[reduceKeyHash] = store;
                }
            }

            return store;
        }

        protected override void LoadValues()
        {
            base.LoadValues();

            TransactionOperationContext context;
            using (_contextPool.AllocateOperationContext(out context))
            using (var tx = context.OpenReadTransaction())
            {
                var mapEntries = tx.InnerTransaction.ReadTree(MapEntriesTreeName);

                if (mapEntries == null)
                    return;

                MapReduceWorkContext.Initialize(mapEntries);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void EnsureValidStats(IndexingStatsScope stats)
        {
            if (_statsInstance == stats)
                return;

            _statsInstance = stats;

            _stats.GetMapEntriesTree = stats.For(IndexingOperation.Reduce.GetMapEntriesTree, start: false);
            _stats.GetMapEntries = stats.For(IndexingOperation.Reduce.GetMapEntries, start: false);
            _stats.RemoveResult = stats.For(IndexingOperation.Reduce.RemoveMapResult, start: false);
            _stats.PutResult = stats.For(IndexingOperation.Reduce.PutMapResult, start: false);
        }

        private class MapPhaseStats
        {
            public IndexingStatsScope RemoveResult;
            public IndexingStatsScope PutResult;
            public IndexingStatsScope GetMapEntriesTree;
            public IndexingStatsScope GetMapEntries;
        }
    }
}