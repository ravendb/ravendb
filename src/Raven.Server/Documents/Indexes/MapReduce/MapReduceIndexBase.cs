using System;
using System.Collections.Generic;
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

namespace Raven.Server.Documents.Indexes.MapReduce
{
    public abstract class MapReduceIndexBase<T> : Index<T> where T : IndexDefinitionBase
    {
        internal const string MapEntriesTreeName = "MapEntries";
        internal const string ResultsStoreTypesTreeName = "ResultsStoreTypes";

        internal readonly MapReduceIndexingContext _mapReduceWorkContext = new MapReduceIndexingContext();
        private FixedSizeTree _documentMapEntries;

        protected MapReduceIndexBase(int indexId, IndexType type, T definition) : base(indexId, type, definition)
        {
        }

        public override IDisposable InitializeIndexingWork(TransactionOperationContext indexContext)
        {
            _mapReduceWorkContext.MapEntries = GetMapEntriesTree(indexContext.Transaction.InnerTransaction);
            _mapReduceWorkContext.ResultsStoreTypes = _mapReduceWorkContext.MapEntries.FixedTreeFor(ResultsStoreTypesTreeName, sizeof(byte));

            _documentMapEntries = new FixedSizeTree(
                   indexContext.Transaction.InnerTransaction.LowLevelTransaction,
                   _mapReduceWorkContext.MapEntries,
                   Slices.Empty,
                   sizeof(ulong),
                   clone: false);

            return _mapReduceWorkContext;
        }

        public override unsafe void HandleDelete(DocumentTombstone tombstone, string collection, IndexWriteOperation writer,
            TransactionOperationContext indexContext, IndexingStatsScope stats)
        {
            Slice docKeyAsSlice;
            using (Slice.External(indexContext.Allocator, tombstone.LoweredKey.Buffer, tombstone.LoweredKey.Length, out docKeyAsSlice))
            {
                _documentMapEntries.RepurposeInstance(docKeyAsSlice, clone: false);

                if (_documentMapEntries.NumberOfEntries == 0)
                    return;

                foreach (var mapEntry in GetMapEntries(_documentMapEntries))
                {
                    var store = GetResultsStore(mapEntry.ReduceKeyHash, indexContext, create: false);

                    store.Delete(mapEntry.Id);
                }

                _mapReduceWorkContext.MapEntries.DeleteFixedTreeFor(tombstone.LoweredKey, sizeof(ulong));
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

        protected unsafe int PutMapResults(LazyStringValue documentKey, IEnumerable<MapResult> mappedResults, TransactionOperationContext indexContext)
        {
            Slice docKeyAsSlice;
            using (Slice.External(indexContext.Allocator, documentKey.Buffer, documentKey.Length, out docKeyAsSlice))
            {
               _documentMapEntries.RepurposeInstance(docKeyAsSlice, clone: false);

                Dictionary<ulong, Queue<long>> existingIdsPerReduceKey = null;

                if (_documentMapEntries.NumberOfEntries > 0)
                {
                    // update operation, let's retrieve ids of existing entries to try to reuse them 

                    existingIdsPerReduceKey = new Dictionary<ulong, Queue<long>>();

                    var mapEntries = GetMapEntries(_documentMapEntries);

                    foreach (var mapEntry in mapEntries)
                    {
                        Queue<long> ids;
                        if (existingIdsPerReduceKey.TryGetValue(mapEntry.ReduceKeyHash, out ids) == false)
                        {
                            ids = new Queue<long>();
                            existingIdsPerReduceKey[mapEntry.ReduceKeyHash] = ids;
                        }

                        ids.Enqueue(mapEntry.Id);
                    }
                }

                int resultsCount = 0;
                foreach (var mapResult in mappedResults)
                {
                    resultsCount++;
                    var reduceKeyHash = mapResult.ReduceKeyHash;

                    long id;

                    Queue<long> availableIds;
                    if (existingIdsPerReduceKey != null &&
                        existingIdsPerReduceKey.TryGetValue(reduceKeyHash, out availableIds))
                    {
                        // reuse id of an old entry
                        id = availableIds.Dequeue();

                        if (availableIds.Count == 0)
                            existingIdsPerReduceKey.Remove(reduceKeyHash);
                    }
                    else
                    {
                        id = _mapReduceWorkContext.GetNextIdentifier();

                        Slice val;
                        using (Slice.External(indexContext.Allocator, (byte*) &reduceKeyHash, sizeof(ulong), out val))
                            _documentMapEntries.Add(id, val);
                    }
                    GetResultsStore(reduceKeyHash, indexContext, true).Add(id, mapResult.Data);
                }

                DocumentDatabase.Metrics.MapReduceMappedPerSecond.Mark(resultsCount);

                if (existingIdsPerReduceKey != null && existingIdsPerReduceKey.Count > 0)
                {
                    // need to remove remaining old entries

                    foreach (var stillExisting in existingIdsPerReduceKey)
                    {
                        var reduceKeyHash = stillExisting.Key;
                        var ids = stillExisting.Value;

                        var oldState = GetResultsStore(reduceKeyHash, indexContext, create: false);

                        while (ids.Count > 0)
                        {
                            var idToDelete = ids.Dequeue();

                            oldState.Delete(idToDelete);

                            _documentMapEntries.Delete(idToDelete);
                        }
                    }
                }

                return resultsCount;
            }
        }

        private static unsafe List<MapEntry> GetMapEntries(FixedSizeTree documentMapEntries)
        {
            var entries = new List<MapEntry>((int)documentMapEntries.NumberOfEntries);

            using (var it = documentMapEntries.Iterate())
            {
                do
                {
                    var currentKey = it.CurrentKey;
                    ulong reduceKeyHash;

                    it.CreateReaderForCurrent().Read((byte*)&reduceKeyHash, sizeof(ulong));

                    entries.Add(new MapEntry
                    {
                        Id = currentKey,
                        ReduceKeyHash = reduceKeyHash
                    });
                } while (it.MoveNext());
            }

            return entries;
        }

        public unsafe MapReduceResultsStore GetResultsStore(ulong reduceKeyHash, TransactionOperationContext indexContext, bool create)
        {
            MapReduceResultsStore store;
            if (_mapReduceWorkContext.StoreByReduceKeyHash.TryGetValue(reduceKeyHash, out store) == false)
            {
                Slice read;
                using (_mapReduceWorkContext.ResultsStoreTypes.Read((long) reduceKeyHash, out read))
                {
                    MapResultsStorageType type;

                    if (read.HasValue)
                        type = (MapResultsStorageType) (*read.CreateReader().Base);
                    else
                        type = MapResultsStorageType.Nested;

                    store = new MapReduceResultsStore(reduceKeyHash, type, indexContext, _mapReduceWorkContext, create);

                    _mapReduceWorkContext.StoreByReduceKeyHash[reduceKeyHash] = store;
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

                _mapReduceWorkContext.Initialize(mapEntries);
            }
        }
    }
}