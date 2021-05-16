using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Raven.Client;
using Raven.Server.Documents.Indexes.MapReduce.Static;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.Documents.Indexes.Static.Counters;
using Raven.Server.Documents.Indexes.Static.TimeSeries;
using Raven.Server.Documents.Indexes.Workers;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json;
using Sparrow.Server;
using Sparrow.Server.Utils;
using Voron;

namespace Raven.Server.Documents.Indexes
{
    public static class StaticIndexHelper
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool IsStaleDueToReferences(MapIndex index, QueryOperationContext queryContext, TransactionOperationContext indexContext, long? referenceCutoff, long? compareExchangeReferenceCutoff, List<string> stalenessReasons)
        {
            return IsStaleDueToReferences(index, index._compiled, queryContext, indexContext, referenceCutoff, compareExchangeReferenceCutoff, stalenessReasons);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool IsStaleDueToReferences(MapCountersIndex index, QueryOperationContext queryContext, TransactionOperationContext indexContext, long? referenceCutoff, long? compareExchangeReferenceCutoff, List<string> stalenessReasons)
        {
            return IsStaleDueToReferences(index, index._compiled, queryContext, indexContext, referenceCutoff, compareExchangeReferenceCutoff, stalenessReasons);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool IsStaleDueToReferences(MapTimeSeriesIndex index, QueryOperationContext queryContext, TransactionOperationContext indexContext, long? referenceCutoff, long? compareExchangeReferenceCutoff, List<string> stalenessReasons)
        {
            return IsStaleDueToReferences(index, index._compiled, queryContext, indexContext, referenceCutoff, compareExchangeReferenceCutoff, stalenessReasons);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool IsStaleDueToReferences(MapReduceIndex index, QueryOperationContext queryContext, TransactionOperationContext indexContext, long? referenceCutoff, long? compareExchangeReferenceCutoff, List<string> stalenessReasons)
        {
            return IsStaleDueToReferences(index, index._compiled, queryContext, indexContext, referenceCutoff, compareExchangeReferenceCutoff, stalenessReasons);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool IsStaleDueToReferences(Index index, AbstractStaticIndexBase compiled, QueryOperationContext queryContext, TransactionOperationContext indexContext, long? referenceCutoff, long? compareExchangeReferenceCutoff, List<string> stalenessReasons)
        {
            foreach (var collection in index.Collections)
            {
                long lastIndexedEtag = -1;

                if (compiled.ReferencedCollections.TryGetValue(collection, out HashSet<CollectionName> referencedCollections))
                {
                    lastIndexedEtag = index._indexStorage.ReadLastIndexedEtag(indexContext.Transaction, collection);

                    // we haven't handled references for that collection yet
                    // in theory we could check what is the last etag for that collection in documents store
                    // but this was checked earlier by the base index class
                    if (lastIndexedEtag > 0)
                    {
                        foreach (var referencedCollection in referencedCollections)
                        {
                            var lastDocEtag = queryContext.Documents.DocumentDatabase.DocumentsStorage.GetLastDocumentEtag(queryContext.Documents.Transaction.InnerTransaction, referencedCollection.Name);
                            var lastProcessedReferenceEtag = index._indexStorage.ReferencesForDocuments.ReadLastProcessedReferenceEtag(indexContext.Transaction.InnerTransaction, collection, referencedCollection);
                            var lastProcessedTombstoneEtag = index._indexStorage.ReferencesForDocuments.ReadLastProcessedReferenceTombstoneEtag(indexContext.Transaction.InnerTransaction, collection, referencedCollection);

                            if (referenceCutoff == null)
                            {
                                if (lastDocEtag > lastProcessedReferenceEtag)
                                {
                                    if (stalenessReasons == null)
                                        return true;

                                    var lastDoc = queryContext.Documents.DocumentDatabase.DocumentsStorage.GetByEtag(queryContext.Documents, lastDocEtag);

                                    stalenessReasons.Add($"There are still some document references to process from collection '{referencedCollection.Name}'. " +
                                                         $"The last document etag in that collection is '{lastDocEtag:#,#;;0}' " +
                                                         $"({Constants.Documents.Metadata.Id}: '{lastDoc.Id}', " +
                                                         $"{Constants.Documents.Metadata.LastModified}: '{lastDoc.LastModified}'), " +
                                                         $"but last processed document etag for that collection is '{lastProcessedReferenceEtag:#,#;;0}'.");
                                }

                                var lastTombstoneEtag = queryContext.Documents.DocumentDatabase.DocumentsStorage.GetLastTombstoneEtag(queryContext.Documents.Transaction.InnerTransaction, referencedCollection.Name);

                                if (lastTombstoneEtag > lastProcessedTombstoneEtag)
                                {
                                    if (stalenessReasons == null)
                                        return true;

                                    var lastTombstone = queryContext.Documents.DocumentDatabase.DocumentsStorage.GetTombstoneByEtag(queryContext.Documents, lastTombstoneEtag);

                                    stalenessReasons.Add($"There are still some tombstone references to process from collection '{referencedCollection.Name}'. " +
                                                         $"The last tombstone etag in that collection is '{lastTombstoneEtag:#,#;;0}' " +
                                                         $"({Constants.Documents.Metadata.Id}: '{lastTombstone.LowerId}', " +
                                                         $"{Constants.Documents.Metadata.LastModified}: '{lastTombstone.LastModified}'), " +
                                                         $"but last processed tombstone etag for that collection is '{lastProcessedTombstoneEtag:#,#;;0}'.");
                                }
                            }
                            else
                            {
                                var minDocEtag = Math.Min(referenceCutoff.Value, lastDocEtag);
                                if (minDocEtag > lastProcessedReferenceEtag)
                                {
                                    if (stalenessReasons == null)
                                        return true;

                                    var lastDoc = queryContext.Documents.DocumentDatabase.DocumentsStorage.GetByEtag(queryContext.Documents, lastDocEtag);

                                    stalenessReasons.Add($"There are still some document references to process from collection '{referencedCollection.Name}'. " +
                                                         $"The last document etag in that collection is '{lastDocEtag:#,#;;0}' " +
                                                         $"({Constants.Documents.Metadata.Id}: '{lastDoc.Id}', " +
                                                         $"{Constants.Documents.Metadata.LastModified}: '{lastDoc.LastModified}') " +
                                                         $"with cutoff set to '{referenceCutoff.Value}', " +
                                                         $"but last processed document etag for that collection is '{lastProcessedReferenceEtag:#,#;;0}'.");
                                }

                                var hasTombstones = queryContext.Documents.DocumentDatabase.DocumentsStorage.HasTombstonesWithEtagGreaterThanStartAndLowerThanOrEqualToEnd(queryContext.Documents, referencedCollection.Name,
                                    lastProcessedTombstoneEtag,
                                    referenceCutoff.Value);

                                if (hasTombstones)
                                {
                                    if (stalenessReasons == null)
                                        return true;

                                    stalenessReasons.Add($"There are still some tombstones to process from collection '{referencedCollection.Name}' with etag range '{lastProcessedTombstoneEtag} - {referenceCutoff.Value}'.");
                                }
                            }
                        }
                    }
                }

                if (compiled.CollectionsWithCompareExchangeReferences.Contains(collection))
                {
                    if (lastIndexedEtag == -1)
                        lastIndexedEtag = index._indexStorage.ReadLastIndexedEtag(indexContext.Transaction, collection);

                    // we haven't handled references for that collection yet
                    // in theory we could check what is the last etag for that collection in documents store
                    // but this was checked earlier by the base index class
                    if (lastIndexedEtag > 0)
                    {
                        var lastCompareExchangeEtag = queryContext.Documents.DocumentDatabase.ServerStore.Cluster.GetLastCompareExchangeIndexForDatabase(queryContext.Server, queryContext.Documents.DocumentDatabase.Name);
                        var lastProcessedReferenceEtag = index._indexStorage.ReferencesForCompareExchange.ReadLastProcessedReferenceEtag(indexContext.Transaction.InnerTransaction, collection, referencedCollection: IndexStorage.CompareExchangeReferences.CompareExchange);

                        if (compareExchangeReferenceCutoff == null)
                        {
                            if (lastCompareExchangeEtag > lastProcessedReferenceEtag)
                            {
                                if (stalenessReasons == null)
                                    return true;

                                stalenessReasons.Add($"There are still some compare exchange references to process for collection '{collection}'. The last compare exchange etag is '{lastCompareExchangeEtag:#,#;;0}', but last processed compare exchange etag for that collection is '{lastProcessedReferenceEtag:#,#;;0}'.");
                            }

                            var lastTombstoneEtag = queryContext.Documents.DocumentDatabase.ServerStore.Cluster.GetLastCompareExchangeTombstoneIndexForDatabase(queryContext.Server, queryContext.Documents.DocumentDatabase.Name);
                            var lastProcessedTombstoneEtag = index._indexStorage.ReferencesForCompareExchange.ReadLastProcessedReferenceTombstoneEtag(indexContext.Transaction.InnerTransaction, collection, referencedCollection: IndexStorage.CompareExchangeReferences.CompareExchange);

                            if (lastTombstoneEtag > lastProcessedTombstoneEtag)
                            {
                                if (stalenessReasons == null)
                                    return true;

                                stalenessReasons.Add($"There are still some compare exchange tombstone references to process for collection '{collection}'. The last compare exchange tombstone etag is '{lastTombstoneEtag:#,#;;0}', but last processed compare exchange tombstone etag for that collection is '{lastProcessedTombstoneEtag:#,#;;0}'.");
                            }
                        }
                        else
                        {
                            var minCompareExchangeEtag = Math.Min(compareExchangeReferenceCutoff.Value, lastCompareExchangeEtag);
                            if (minCompareExchangeEtag > lastProcessedReferenceEtag)
                            {
                                if (stalenessReasons == null)
                                    return true;

                                stalenessReasons.Add($"There are still some compare exchange references to process for collection '{collection}'. The last compare exchange etag is '{lastCompareExchangeEtag:#,#;;0}' with cutoff set to '{compareExchangeReferenceCutoff.Value}', but last processed compare exchange etag for that collection is '{lastProcessedReferenceEtag:#,#;;0}'.");
                            }

                            var lastProcessedTombstoneEtag = index._indexStorage.ReferencesForCompareExchange.ReadLastProcessedReferenceTombstoneEtag(indexContext.Transaction.InnerTransaction, collection, referencedCollection: IndexStorage.CompareExchangeReferences.CompareExchange);
                            var hasTombstones = queryContext.Documents.DocumentDatabase.ServerStore.Cluster.HasCompareExchangeTombstonesWithEtagGreaterThanStartAndLowerThanOrEqualToEnd(queryContext.Server, queryContext.Documents.DocumentDatabase.Name,
                                    lastProcessedTombstoneEtag,
                                    compareExchangeReferenceCutoff.Value);

                            if (hasTombstones)
                            {
                                if (stalenessReasons == null)
                                    return true;

                                stalenessReasons.Add($"There are still some compare exchange tombstones to process for collection '{collection}' with etag range '{lastProcessedTombstoneEtag} - {compareExchangeReferenceCutoff.Value}'.");
                            }
                        }
                    }
                }
            }

            return stalenessReasons?.Count > 0;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static unsafe long CalculateIndexEtag(Index index, AbstractStaticIndexBase compiled, int length, byte* indexEtagBytes, byte* writePos, QueryOperationContext queryContext, TransactionOperationContext indexContext)
        {
            foreach (var collection in index.Collections)
            {
                if (compiled.ReferencedCollections.TryGetValue(collection, out HashSet<CollectionName> referencedCollections))
                {
                    foreach (var referencedCollection in referencedCollections)
                    {
                        var lastDocEtag = queryContext.Documents.DocumentDatabase.DocumentsStorage.GetLastDocumentEtag(queryContext.Documents.Transaction.InnerTransaction, referencedCollection.Name);
                        var lastProcessedReferenceEtag = index._indexStorage.ReferencesForDocuments.ReadLastProcessedReferenceEtag(indexContext.Transaction.InnerTransaction, collection, referencedCollection);
                        
                        var lastTombstoneEtag = queryContext.Documents.DocumentDatabase.DocumentsStorage.GetLastTombstoneEtag(queryContext.Documents.Transaction.InnerTransaction, referencedCollection.Name);
                        var lastProcessedTombstoneEtag = index._indexStorage.ReferencesForDocuments.ReadLastProcessedReferenceTombstoneEtag(indexContext.Transaction.InnerTransaction, collection, referencedCollection);

                        *(long*)writePos = lastDocEtag;
                        writePos += sizeof(long);
                        *(long*)writePos = lastProcessedReferenceEtag;
                        writePos += sizeof(long);
                        *(long*)writePos = lastTombstoneEtag;
                        writePos += sizeof(long);
                        *(long*)writePos = lastProcessedTombstoneEtag;

                        var referencesInfo = index.GetInMemoryReferencesState(collection, isCompareExchange: false);
                        writePos += sizeof(long);
                        *(long*)writePos = referencesInfo.ParentItemEtag;
                        writePos += sizeof(long);
                        *(long*)writePos = referencesInfo.ParentTombstoneEtag;
                    }
                }

                if (compiled.CollectionsWithCompareExchangeReferences.Contains(collection))
                {
                    var lastCompareExchangeEtag = queryContext.Documents.DocumentDatabase.ServerStore.Cluster.GetLastCompareExchangeIndexForDatabase(queryContext.Server, queryContext.Documents.DocumentDatabase.Name);
                    var lastProcessedReferenceEtag = index._indexStorage.ReferencesForCompareExchange.ReadLastProcessedReferenceEtag(indexContext.Transaction.InnerTransaction, collection, referencedCollection: IndexStorage.CompareExchangeReferences.CompareExchange);

                    var lastTombstoneEtag = queryContext.Documents.DocumentDatabase.ServerStore.Cluster.GetLastCompareExchangeTombstoneIndexForDatabase(queryContext.Server, queryContext.Documents.DocumentDatabase.Name);
                    var lastProcessedTombstoneEtag = index._indexStorage.ReferencesForCompareExchange.ReadLastProcessedReferenceTombstoneEtag(indexContext.Transaction.InnerTransaction, collection, referencedCollection: IndexStorage.CompareExchangeReferences.CompareExchange);

                    *(long*)writePos = lastCompareExchangeEtag;
                    writePos += sizeof(long);
                    *(long*)writePos = lastProcessedReferenceEtag;
                    writePos += sizeof(long);
                    *(long*)writePos = lastTombstoneEtag;
                    writePos += sizeof(long);
                    *(long*)writePos = lastProcessedTombstoneEtag;

                    var referencesInfo = index.GetInMemoryReferencesState(collection, isCompareExchange: true);
                    writePos += sizeof(long);
                    *(long*)writePos = referencesInfo.ParentItemEtag;
                    writePos += sizeof(long);
                    *(long*)writePos = referencesInfo.ParentTombstoneEtag;
                }
            }

            unchecked
            {
                return (long)Hashing.XXHash64.Calculate(indexEtagBytes, (ulong)length);
            }
        }

        public static Dictionary<string, long> GetLastProcessedDocumentTombstonesPerCollection(
            Index index, HashSet<string> referencedCollections, IEnumerable<string> collections,
            Dictionary<string, HashSet<CollectionName>> compiledReferencedCollections,
            IndexStorage indexStorage)
        {
            using (index._contextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (var tx = context.OpenReadTransaction())
            {
                var etags = index.GetLastProcessedDocumentTombstonesPerCollection(tx);

                if (referencedCollections.Count <= 0)
                    return etags;

                foreach (var collection in collections)
                {
                    if (compiledReferencedCollections.TryGetValue(collection, out HashSet<CollectionName> collectionNames) == false)
                        continue;

                    foreach (var collectionName in collectionNames)
                    {
                        var etag = indexStorage.ReferencesForDocuments.ReadLastProcessedReferenceTombstoneEtag(tx.InnerTransaction, collection, collectionName);
                        if (etags.TryGetValue(collectionName.Name, out long currentEtag) == false || etag < currentEtag)
                            etags[collectionName.Name] = etag;
                    }
                }

                return etags;
            }
        }

        public static (long? LastProcessedCompareExchangeReferenceEtag, long? LastProcessedCompareExchangeReferenceTombstoneEtag) GetLastProcessedCompareExchangeReferenceEtags(Index index, AbstractStaticIndexBase compiled, TransactionOperationContext indexContext)
        {
            long? lastProcessedCompareExchangeReferenceEtag = null;
            long? lastProcessedCompareExchangeReferenceTombstoneEtag = null;
            foreach (var collection in compiled.CollectionsWithCompareExchangeReferences)
            {
                var lastProcessedCompareExchangeReferenceEtagForCollection = index._indexStorage.ReferencesForCompareExchange.ReadLastProcessedReferenceEtag(indexContext.Transaction.InnerTransaction, collection, IndexStorage.CompareExchangeReferences.CompareExchange);
                lastProcessedCompareExchangeReferenceEtag = lastProcessedCompareExchangeReferenceEtag.HasValue
                    ? Math.Max(lastProcessedCompareExchangeReferenceEtag.Value, lastProcessedCompareExchangeReferenceEtagForCollection)
                    : lastProcessedCompareExchangeReferenceEtagForCollection;

                var lastProcessedCompareExchangeReferenceTombstoneEtagForCollection = index._indexStorage.ReferencesForCompareExchange.ReadLastProcessedReferenceTombstoneEtag(indexContext.Transaction.InnerTransaction, collection, IndexStorage.CompareExchangeReferences.CompareExchange);
                lastProcessedCompareExchangeReferenceTombstoneEtag = lastProcessedCompareExchangeReferenceTombstoneEtag.HasValue
                    ? Math.Max(lastProcessedCompareExchangeReferenceTombstoneEtag.Value, lastProcessedCompareExchangeReferenceTombstoneEtagForCollection)
                    : lastProcessedCompareExchangeReferenceTombstoneEtagForCollection;
            }

            return (lastProcessedCompareExchangeReferenceEtag, lastProcessedCompareExchangeReferenceTombstoneEtag);
        }

        internal static Dictionary<string, long> GetLastProcessedEtagsPerCollection(Index index, HashSet<string> collections, IndexStorage indexStorage)
        {
            using (index._contextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (var tx = context.OpenReadTransaction())
            {
                var etags = new Dictionary<string, long>(StringComparer.OrdinalIgnoreCase);
                foreach (var collection in collections)
                {
                    etags[collection] = indexStorage.ReadLastIndexedEtag(tx, collection);
                }

                return etags;
            }
        }

        public static bool ShouldReplace(Index index, ref bool? isSideBySide)
        {
            if (isSideBySide.HasValue == false)
                isSideBySide = index.Name.StartsWith(Constants.Documents.Indexing.SideBySideIndexNamePrefix, StringComparison.OrdinalIgnoreCase);

            if (isSideBySide == false)
                return false;

            using (var context = QueryOperationContext.Allocate(index.DocumentDatabase, index))
            using (index._contextPool.AllocateOperationContext(out TransactionOperationContext indexContext))
            {
                using (indexContext.OpenReadTransaction())
                using (context.OpenReadTransaction())
                {
                    indexContext.IgnoreStalenessDueToReduceOutputsToDelete = true;

                    try
                    {
                        var canReplace = index.IsStale(context, indexContext) == false;
                        if (canReplace)
                            isSideBySide = null;

                        return canReplace;
                    }
                    finally
                    {
                        indexContext.IgnoreStalenessDueToReduceOutputsToDelete = false;
                    }
                }
            }
        }

        public static void HandleDeleteBySourceDocumentId(MapReduceIndex index, HandleReferences handleReferences, HandleCompareExchangeReferences handleCompareExchangeReferences, Tombstone tombstone, string collection, IndexWriteOperation writer, TransactionOperationContext indexContext, IndexingStatsScope stats)
        {
            HandleReferencesDelete(handleReferences, handleCompareExchangeReferences, tombstone, collection, writer, indexContext, stats);

            using (ToPrefixKey(tombstone.LowerId, indexContext, out var prefixKey))
            {
                var toDelete = new List<Slice>();

                using (var it = index.MapReduceWorkContext.MapPhaseTree.Iterate(prefetch: false))
                {
                    it.SetRequiredPrefix(prefixKey);

                    if (it.Seek(prefixKey) == false)
                        return;

                    do
                    {
                        toDelete.Add(it.CurrentKey.Clone(indexContext.Allocator));
                    } while (it.MoveNext());
                }

                foreach (var key in toDelete)
                {
                    index.MapReduceWorkContext.DocumentMapEntries.RepurposeInstance(key, clone: false);

                    if (index.MapReduceWorkContext.DocumentMapEntries.NumberOfEntries == 0)
                        continue;

                    foreach (var mapEntry in MapReduceIndex.GetMapEntries(index.MapReduceWorkContext.DocumentMapEntries))
                    {
                        var store = index.GetResultsStore(mapEntry.ReduceKeyHash, indexContext, create: false);

                        store.Delete(mapEntry.Id);
                    }

                    index.MapReduceWorkContext.MapPhaseTree.DeleteFixedTreeFor(key, sizeof(ulong));
                }
            }

            static unsafe ByteStringContext.InternalScope ToPrefixKey(LazyStringValue key, TransactionOperationContext context, out Slice prefixKey)
            {
                var scope = context.Allocator.Allocate(key.Size + 1, out ByteString keyMem);

                Memory.Copy(keyMem.Ptr, key.Buffer, key.Size);
                keyMem.Ptr[key.Size] = SpecialChars.LuceneRecordSeparator;

                prefixKey = new Slice(SliceOptions.Key, keyMem);
                return scope;
            }
        }

        public static void HandleDeleteBySourceDocument(HandleReferences handleReferences, HandleCompareExchangeReferences handleCompareExchangeReferences, Tombstone tombstone, string collection, IndexWriteOperation writer, TransactionOperationContext indexContext, IndexingStatsScope stats)
        {
            HandleReferencesDelete(handleReferences, handleCompareExchangeReferences, tombstone, collection, writer, indexContext, stats);

            HandleDeleteBySourceDocument(tombstone, writer, stats);
        }

        public static void HandleReferencesDelete(HandleReferences handleReferences, HandleCompareExchangeReferences handleCompareExchangeReferences, Tombstone tombstone, string collection, IndexWriteOperation writer, TransactionOperationContext indexContext, IndexingStatsScope stats)
        {
            if (handleCompareExchangeReferences != null)
                handleCompareExchangeReferences.HandleDelete(tombstone, collection, writer, indexContext, stats);

            if (handleReferences != null)
                handleReferences.HandleDelete(tombstone, collection, writer, indexContext, stats);
        }

        private static void HandleDeleteBySourceDocument(Tombstone tombstone, IndexWriteOperation writer, IndexingStatsScope stats)
        {
            writer.DeleteBySourceDocument(tombstone.LowerId, stats);
        }
    }
}
