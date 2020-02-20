using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Raven.Client;
using Raven.Server.Documents.Indexes.MapReduce.Static;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.Documents.Indexes.Static.TimeSeries;
using Raven.Server.ServerWide.Context;
using Sparrow;

namespace Raven.Server.Documents.Indexes
{
    public static class StaticIndexHelper
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool IsStaleDueToReferences(MapIndex index, QueryOperationContext queryContext, TransactionOperationContext indexContext, long? referenceCutoff, List<string> stalenessReasons)
        {
            return IsStaleDueToReferences(index, index._compiled, queryContext, indexContext, referenceCutoff, stalenessReasons);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool IsStaleDueToReferences(MapTimeSeriesIndex index, QueryOperationContext queryContext, TransactionOperationContext indexContext, long? referenceCutoff, List<string> stalenessReasons)
        {
            return IsStaleDueToReferences(index, index._compiled, queryContext, indexContext, referenceCutoff, stalenessReasons);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool IsStaleDueToReferences(MapReduceIndex index, QueryOperationContext queryContext, TransactionOperationContext indexContext, long? referenceCutoff, List<string> stalenessReasons)
        {
            return IsStaleDueToReferences(index, index._compiled, queryContext, indexContext, referenceCutoff, stalenessReasons);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static unsafe long CalculateIndexEtag(MapIndex index, int length, byte* indexEtagBytes, byte* writePos, QueryOperationContext queryContext, TransactionOperationContext indexContext)
        {
            return CalculateIndexEtag(index, index._compiled, length, indexEtagBytes, writePos, queryContext, indexContext);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static unsafe long CalculateIndexEtag(MapTimeSeriesIndex index, int length, byte* indexEtagBytes, byte* writePos, QueryOperationContext queryContext, TransactionOperationContext indexContext)
        {
            return CalculateIndexEtag(index, index._compiled, length, indexEtagBytes, writePos, queryContext, indexContext);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static unsafe long CalculateIndexEtag(MapReduceIndex index, int length, byte* indexEtagBytes, byte* writePos, QueryOperationContext queryContext, TransactionOperationContext indexContext)
        {
            return CalculateIndexEtag(index, index._compiled, length, indexEtagBytes, writePos, queryContext, indexContext);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool IsStaleDueToReferences(Index index, AbstractStaticIndexBase compiled, QueryOperationContext queryContext, TransactionOperationContext indexContext, long? referenceCutoff, List<string> stalenessReasons)
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
                            var lastDocEtag = queryContext.Documents.DocumentDatabase.DocumentsStorage.GetLastDocumentEtag(queryContext.Documents, referencedCollection.Name);
                            var lastProcessedReferenceEtag = index._indexStorage.ReferencesForDocuments.ReadLastProcessedReferenceEtag(indexContext.Transaction, collection, referencedCollection);

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

                                var lastTombstoneEtag = queryContext.Documents.DocumentDatabase.DocumentsStorage.GetLastTombstoneEtag(queryContext.Documents, referencedCollection.Name);
                                var lastProcessedTombstoneEtag = index._indexStorage.ReferencesForDocuments.ReadLastProcessedReferenceTombstoneEtag(indexContext.Transaction, collection, referencedCollection);

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

                                var lastProcessedTombstoneEtag = index._indexStorage.ReferencesForDocuments.ReadLastProcessedReferenceTombstoneEtag(indexContext.Transaction, collection, referencedCollection);
                                var hasTombstones = queryContext.Documents.DocumentDatabase.DocumentsStorage.HasTombstonesWithEtagGreaterThanStartAndLowerThanOrEqualToEnd(queryContext.Documents, referencedCollection.Name,
                                    lastProcessedTombstoneEtag,
                                    referenceCutoff.Value);

                                if (hasTombstones)
                                {
                                    if (stalenessReasons == null)
                                        return true;

                                    stalenessReasons.Add($"There are still tombstones to process from collection '{referencedCollection.Name}' with etag range '{lastProcessedTombstoneEtag} - {referenceCutoff.Value}'.");
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
                        var lastProcessedReferenceEtag = index._indexStorage.ReferencesForCompareExchange.ReadLastProcessedReferenceEtag(indexContext.Transaction, collection, referencedCollection: IndexStorage.CompareExchangeReferences.CompareExchange);

                        if (lastCompareExchangeEtag > lastProcessedReferenceEtag)
                        {
                            if (stalenessReasons == null)
                                return true;

                            stalenessReasons.Add($"There are still some compare exchange references to process for collection '{collection}'. The last compare exchange etag is '{lastCompareExchangeEtag:#,#;;0}' but last processed compare exchange etag for that collection is '{lastProcessedReferenceEtag:#,#;;0}'.");
                        }

                        var lastTombstoneEtag = queryContext.Documents.DocumentDatabase.ServerStore.Cluster.GetLastCompareExchangeTombstoneIndexForDatabase(queryContext.Server, queryContext.Documents.DocumentDatabase.Name);
                        var lastProcessedTombstoneEtag = index._indexStorage.ReferencesForCompareExchange.ReadLastProcessedReferenceTombstoneEtag(indexContext.Transaction, collection, referencedCollection: IndexStorage.CompareExchangeReferences.CompareExchange);

                        if (lastTombstoneEtag > lastProcessedTombstoneEtag)
                        {
                            if (stalenessReasons == null)
                                return true;

                            stalenessReasons.Add($"There are still some compare exchange tombstone references to process for collection '{collection}'. The last compare exchange tombstone etag is '{lastTombstoneEtag:#,#;;0}' but last processed compare exchange tombstone etag for that collection is '{lastProcessedTombstoneEtag:#,#;;0}'.");
                        }
                    }
                }
            }

            return stalenessReasons?.Count > 0;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static unsafe long CalculateIndexEtag(Index index, AbstractStaticIndexBase compiled, int length, byte* indexEtagBytes, byte* writePos, QueryOperationContext queryContext, TransactionOperationContext indexContext)
        {
            foreach (var collection in index.Collections)
            {
                if (compiled.ReferencedCollections.TryGetValue(collection, out HashSet<CollectionName> referencedCollections))
                {
                    foreach (var referencedCollection in referencedCollections)
                    {
                        var lastDocEtag = queryContext.Documents.DocumentDatabase.DocumentsStorage.GetLastDocumentEtag(queryContext.Documents, referencedCollection.Name);
                        var lastProcessedReferenceEtag = index._indexStorage.ReferencesForDocuments.ReadLastProcessedReferenceEtag(indexContext.Transaction, collection, referencedCollection);

                        var lastTombstoneEtag = queryContext.Documents.DocumentDatabase.DocumentsStorage.GetLastTombstoneEtag(queryContext.Documents, referencedCollection.Name);
                        var lastProcessedTombstoneEtag = index._indexStorage.ReferencesForDocuments.ReadLastProcessedReferenceTombstoneEtag(indexContext.Transaction, collection, referencedCollection);

                        *(long*)writePos = lastDocEtag;
                        writePos += sizeof(long);
                        *(long*)writePos = lastProcessedReferenceEtag;
                        writePos += sizeof(long);
                        *(long*)writePos = lastTombstoneEtag;
                        writePos += sizeof(long);
                        *(long*)writePos = lastProcessedTombstoneEtag;
                    }
                }

                if (compiled.CollectionsWithCompareExchangeReferences.Contains(collection))
                {
                    var lastCompareExchangeEtag = queryContext.Documents.DocumentDatabase.ServerStore.Cluster.GetLastCompareExchangeIndexForDatabase(queryContext.Server, queryContext.Documents.DocumentDatabase.Name);
                    var lastProcessedReferenceEtag = index._indexStorage.ReferencesForCompareExchange.ReadLastProcessedReferenceEtag(indexContext.Transaction, collection, referencedCollection: IndexStorage.CompareExchangeReferences.CompareExchange);

                    var lastTombstoneEtag = queryContext.Documents.DocumentDatabase.ServerStore.Cluster.GetLastCompareExchangeTombstoneIndexForDatabase(queryContext.Server, queryContext.Documents.DocumentDatabase.Name);
                    var lastProcessedTombstoneEtag = index._indexStorage.ReferencesForCompareExchange.ReadLastProcessedReferenceTombstoneEtag(indexContext.Transaction, collection, referencedCollection: IndexStorage.CompareExchangeReferences.CompareExchange);

                    *(long*)writePos = lastCompareExchangeEtag;
                    writePos += sizeof(long);
                    *(long*)writePos = lastProcessedReferenceEtag;
                    writePos += sizeof(long);
                    *(long*)writePos = lastTombstoneEtag;
                    writePos += sizeof(long);
                    *(long*)writePos = lastProcessedTombstoneEtag;
                }
            }

            unchecked
            {
                return (long)Hashing.XXHash64.Calculate(indexEtagBytes, (ulong)length);
            }
        }

        public static Dictionary<string, long> GetLastProcessedTombstonesPerCollection(
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
                        var etag = indexStorage.ReferencesForDocuments.ReadLastProcessedReferenceTombstoneEtag(tx, collection, collectionName);
                        if (etags.TryGetValue(collectionName.Name, out long currentEtag) == false || etag < currentEtag)
                            etags[collectionName.Name] = etag;
                    }
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

            using (var context = QueryOperationContext.ForIndex(index))
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
    }
}
