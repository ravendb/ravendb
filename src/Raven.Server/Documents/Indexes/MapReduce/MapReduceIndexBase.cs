using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Includes;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.Results;
using Raven.Server.Documents.Queries.Timings;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json;
using Voron;
using Voron.Data.BTrees;
using Voron.Data.Fixed;
using Voron.Debugging;
using Voron.Impl;
using Voron.Util;

namespace Raven.Server.Documents.Indexes.MapReduce
{
    public abstract class MapReduceIndexBase<T, TField> : Index<T, TField> where T : IndexDefinitionBaseServerSide<TField> where TField : IndexFieldBase
    {
        internal const string MapPhaseTreeName = "MapPhaseTree";
        internal const string ReducePhaseTreeName = "ReducePhaseTree";
        internal const string ResultsStoreTypesTreeName = "ResultsStoreTypes";

        internal readonly MapReduceIndexingContext MapReduceWorkContext = new MapReduceIndexingContext();

        private IndexingStatsScope _statsInstance;
        private readonly MapPhaseStats _stats = new MapPhaseStats();

        protected MapReduceIndexBase(IndexType type, IndexSourceType sourceType, T definition) : base(type, sourceType, definition)
        {
        }

        public override IDisposable InitializeIndexingWork(TransactionOperationContext indexContext)
        {
            MapReduceWorkContext.MapPhaseTree = GetMapPhaseTree(indexContext.Transaction.InnerTransaction);
            MapReduceWorkContext.ReducePhaseTree = GetReducePhaseTree(indexContext.Transaction.InnerTransaction);
            MapReduceWorkContext.ResultsStoreTypes = MapReduceWorkContext.ReducePhaseTree.FixedTreeFor(ResultsStoreTypesTreeName, sizeof(byte));

            MapReduceWorkContext.DocumentMapEntries = new FixedSizeTree(
                   indexContext.Transaction.InnerTransaction.LowLevelTransaction,
                   MapReduceWorkContext.MapPhaseTree,
                   Slices.Empty,
                   sizeof(ulong),
                   clone: false);

            return MapReduceWorkContext;
        }

        public override unsafe void HandleDelete(Tombstone tombstone, string collection, IndexWriteOperation writer,
            TransactionOperationContext indexContext, IndexingStatsScope stats)
        {
            using (Slice.External(indexContext.Allocator, tombstone.LowerId, out Slice docKeyAsSlice))
            {
                MapReduceWorkContext.DocumentMapEntries.RepurposeInstance(docKeyAsSlice, clone: false);

                if (MapReduceWorkContext.DocumentMapEntries.NumberOfEntries == 0)
                    return;

                foreach (var mapEntry in GetMapEntries(MapReduceWorkContext.DocumentMapEntries))
                {
                    var store = GetResultsStore(mapEntry.ReduceKeyHash, indexContext, create: false);

                    store.Delete(mapEntry.Id);
                }

                MapReduceWorkContext.MapPhaseTree.DeleteFixedTreeFor(tombstone.LowerId, sizeof(ulong));
            }
        }

        public override IQueryResultRetriever GetQueryResultRetriever(IndexQueryServerSide query, QueryTimingsScope queryTimings, DocumentsOperationContext documentsContext, FieldsToFetch fieldsToFetch, IncludeDocumentsCommand includeDocumentsCommand, IncludeCompareExchangeValuesCommand includeCompareExchangeValuesCommand)
        {
            return new MapReduceQueryResultRetriever(DocumentDatabase, query, queryTimings, DocumentDatabase.DocumentsStorage, documentsContext, fieldsToFetch, includeDocumentsCommand, includeCompareExchangeValuesCommand);
        }

        private static Tree GetMapPhaseTree(Transaction tx)
        {
            // MapPhase tree has the following entries
            // 1) { document key, fixed size tree } where each fixed size tree stores records like
            //   |----> { identifier of a map result, hash of a reduce key for the map result }
            // 2) entry to keep track the next identifier of a next map result { #NextMapResultId, long_value }

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

        protected unsafe int PutMapResults(LazyStringValue lowerId, LazyStringValue sourceDocumentId, IEnumerable<MapResult> mappedResults, TransactionOperationContext indexContext, IndexingStatsScope stats)
        {
            EnsureValidStats(stats);

            using (Slice.External(indexContext.Allocator, lowerId, out Slice docIdAsSlice))
            {
                Queue<MapEntry> existingEntries = null;

                using (_stats.GetMapEntriesTree.Start())
                    MapReduceWorkContext.DocumentMapEntries.RepurposeInstance(docIdAsSlice, clone: false);

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

                        long mapEntryId = -1;

                        if (existingEntries?.Count > 0)
                        {
                            var existing = existingEntries.Dequeue();
                            var storeOfExisting = GetResultsStore(existing.ReduceKeyHash, indexContext, false);

                            if (reduceKeyHash == existing.ReduceKeyHash)
                            {
                                using (var existingResult = storeOfExisting.Get(existing.Id))
                                {
                                    if (ResultsBinaryEqual(mapResult.Data, existingResult.Data))
                                    {
                                        continue;
                                    }
                                }

                                mapEntryId = existing.Id;
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
                            if (mapEntryId == -1)
                            {
                                mapEntryId = MapReduceWorkContext.NextMapResultId++;

                                using (Slice.External(indexContext.Allocator, (byte*)&reduceKeyHash, sizeof(ulong), out Slice val))
                                    MapReduceWorkContext.DocumentMapEntries.Add(mapEntryId, val);
                            }

                            GetResultsStore(reduceKeyHash, indexContext, create: true).Add(mapEntryId, mapResult.Data);
                        }
                    }
                }

                HandleIndexOutputsPerDocument(sourceDocumentId ?? lowerId, resultsCount, stats);

                DocumentDatabase.Metrics.MapReduceIndexes.MappedPerSec.Mark(resultsCount);

                while (existingEntries?.Count > 0)
                {
                    // need to remove remaining old entries

                    var oldResult = existingEntries.Dequeue();

                    var oldState = GetResultsStore(oldResult.ReduceKeyHash, indexContext, create: false);

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

            if (documentMapEntries.NumberOfEntries == 0)
                return entries;

            using (var it = documentMapEntries.Iterate())
            {
                if (it.Seek(long.MinValue) == false)
                    ThrowCouldNotSeekToFirstElement(documentMapEntries.Name);

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

        private static void ThrowCouldNotSeekToFirstElement(Slice treeName)
        {
            throw new InvalidOperationException($"Could not seek to the first element of {treeName} tree");
        }

        public MapReduceResultsStore GetResultsStore(ulong reduceKeyHash, TransactionOperationContext indexContext, bool create)
        {
            if (MapReduceWorkContext.StoreByReduceKeyHash.TryGetValue(reduceKeyHash, out MapReduceResultsStore store) == false)
            {
                MapReduceWorkContext.StoreByReduceKeyHash[reduceKeyHash] = store =
                    CreateResultsStore(MapReduceWorkContext.ResultsStoreTypes, reduceKeyHash, indexContext, create);
            }

            return store;
        }

        internal unsafe MapReduceResultsStore CreateResultsStore(FixedSizeTree typePerHash, ulong reduceKeyHash,
            TransactionOperationContext indexContext, bool create)
        {
            MapResultsStorageType type;
            using (typePerHash.Read((long)reduceKeyHash, out Slice read))
            {
                if (read.HasValue)
                    type = (MapResultsStorageType)(*read.CreateReader().Base);
                else
                    type = MapResultsStorageType.Nested;
            }

            return new MapReduceResultsStore(reduceKeyHash, type, indexContext, MapReduceWorkContext, create);
        }

        protected override void LoadValues()
        {
            base.LoadValues();

            using (_contextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (var tx = context.OpenReadTransaction())
            {
                var mapEntries = tx.InnerTransaction.ReadTree(MapPhaseTreeName);

                if (mapEntries == null)
                    return;

                MapReduceWorkContext.Initialize(mapEntries);
            }
        }

        public override DetailedStorageReport GenerateStorageReport(bool calculateExactSizes)
        {
            using (_contextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (var tx = context.OpenReadTransaction())
            {
                var report = _indexStorage.Environment().GenerateDetailedReport(tx.InnerTransaction, calculateExactSizes);

                var treesToKeep = new List<TreeReport>();

                TreeReport aggregatedTree = null;
                var numberOfReduceTrees = 0;
                foreach (var treeReport in report.Trees)
                {
                    if (treeReport.Name.StartsWith(MapReduceResultsStore.ReduceTreePrefix) == false)
                    {
                        treesToKeep.Add(treeReport);
                        continue;
                    }

                    numberOfReduceTrees++;

                    if (aggregatedTree == null)
                        aggregatedTree = new TreeReport();

                    aggregatedTree.AllocatedSpaceInBytes += treeReport.AllocatedSpaceInBytes;
                    aggregatedTree.BranchPages += treeReport.BranchPages;
                    aggregatedTree.Density = calculateExactSizes ? aggregatedTree.Density + treeReport.Density : -1;
                    aggregatedTree.Depth = Math.Max(aggregatedTree.Depth, treeReport.Depth);
                    aggregatedTree.LeafPages += treeReport.LeafPages;
                    aggregatedTree.NumberOfEntries += treeReport.NumberOfEntries;
                    aggregatedTree.OverflowPages += treeReport.OverflowPages;
                    aggregatedTree.PageCount += treeReport.PageCount;
                    aggregatedTree.Type = treeReport.Type;
                    aggregatedTree.UsedSpaceInBytes = calculateExactSizes ? aggregatedTree.UsedSpaceInBytes + treeReport.UsedSpaceInBytes : -1;
                }

                if (aggregatedTree != null)
                {
                    aggregatedTree.Name = $"Reduce Trees (#{numberOfReduceTrees})";
                    treesToKeep.Add(aggregatedTree);
                }

                report.Trees = treesToKeep;

                return report;
            }
        }

        public override void SaveLastState()
        {
            throw new NotSupportedException();
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
