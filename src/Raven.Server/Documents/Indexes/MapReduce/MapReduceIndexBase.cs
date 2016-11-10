using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Raven.Client.Data.Indexes;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.Results;
using Raven.Server.ServerWide;
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
        internal const string MapPhaseTreeName = "MapPhaseTree";
        internal const string ReducePhaseTreeName = "ReducePhaseTree";
        internal const string ResultsStoreTypesTreeName = "ResultsStoreTypes";
        
        private readonly MapPhaseStats _stats = new MapPhaseStats();
        internal readonly MapReduceIndexingContext MapReduceWorkContext = new MapReduceIndexingContext();

        private IndexingStatsScope _statsInstance;
        private PageLocator _pageLocator;
        
        protected MapReduceIndexBase(int indexId, IndexType type, T definition) : base(indexId, type, definition)
        {
        }

        public override IDisposable InitializeIndexingWork(TransactionOperationContext indexContext)
        {
            MapReduceWorkContext.MapPhaseTree = GetMapPhaseTree(indexContext.Transaction.InnerTransaction);
            MapReduceWorkContext.ReducePhaseTree = GetReducePhaseTree(indexContext.Transaction.InnerTransaction);
            MapReduceWorkContext.ResultsStoreTypes = MapReduceWorkContext.ReducePhaseTree.FixedTreeFor(ResultsStoreTypesTreeName, sizeof(byte));

            _pageLocator = new PageLocator(indexContext.Transaction.InnerTransaction.LowLevelTransaction, 128);

            MapReduceWorkContext.DocumentMapEntries = new FixedSizeTree(
                   indexContext.Transaction.InnerTransaction.LowLevelTransaction,
                   MapReduceWorkContext.MapPhaseTree,
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

                MapReduceWorkContext.MapPhaseTree.DeleteFixedTreeFor(tombstone.LoweredKey, sizeof(ulong));
            }


        }

        public override IQueryResultRetriever GetQueryResultRetriever(DocumentsOperationContext documentsContext, FieldsToFetch fieldsToFetch)
        {
            return new MapReduceQueryResultRetriever(documentsContext, fieldsToFetch);
        }

        private static Tree GetMapPhaseTree(Transaction tx)
        {
            // MapPhase tree has the following entries
            // 1) { document key, fixed size tree } where each fixed size tree stores records like 
            //   |----> { identifier of a map result, hash of a reduce key for the map result }
            // 2) entry to keep track the identifier of a last stored entry { #LastMapResultId, long_value }

            return tx.CreateTree(MapPhaseTreeName);
        }

        private static Tree GetReducePhaseTree(Transaction tx)
        {
            // ReducePhase tree has the following entries
            // 1) fixed size tree called which stores records like
            //    |----> { reduce key hash, MapResultsStorageType enum } 
            // 2) { #reduceValues- hash of a reduce key, nested values section } 

            return tx.CreateTree(ReducePhaseTreeName);
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

        internal static unsafe Queue<MapEntry> GetMapEntries(FixedSizeTree documentMapEntries)
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

        public MapReduceResultsStore GetResultsStore(ulong reduceKeyHash, TransactionOperationContext indexContext, bool create, PageLocator pageLocator = null)
        {
            MapReduceResultsStore store;
            if (MapReduceWorkContext.StoreByReduceKeyHash.TryGetValue(reduceKeyHash, out store) == false)
            {
                MapReduceWorkContext.StoreByReduceKeyHash[reduceKeyHash] = store = 
                    CreateResultsStore(MapReduceWorkContext.ResultsStoreTypes, reduceKeyHash, indexContext, create, pageLocator);
            }

            return store;
        }

        internal unsafe MapReduceResultsStore CreateResultsStore(FixedSizeTree typePerHash, ulong reduceKeyHash,
            TransactionOperationContext indexContext, bool create, PageLocator pageLocator = null)
        {
            MapResultsStorageType type;
            Slice read;
            using (typePerHash.Read((long) reduceKeyHash, out read))
            {
                if (read.HasValue)
                    type = (MapResultsStorageType) (*read.CreateReader().Base);
                else
                    type = MapResultsStorageType.Nested;
            }

            return new MapReduceResultsStore(reduceKeyHash, type, indexContext, MapReduceWorkContext, create,
                pageLocator);
        }

        protected override void LoadValues()
        {
            base.LoadValues();

            TransactionOperationContext context;
            using (_contextPool.AllocateOperationContext(out context))
            using (var tx = context.OpenReadTransaction())
            {
                var mapEntries = tx.InnerTransaction.ReadTree(MapPhaseTreeName);

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