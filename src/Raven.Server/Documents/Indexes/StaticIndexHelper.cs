using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Raven.Client;
using Raven.Server.Documents.Indexes.MapReduce.Static;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.ServerWide.Context;
using Sparrow;

namespace Raven.Server.Documents.Indexes
{
    public static class StaticIndexHelper
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool IsStaleDueToReferences(MapIndex index, DocumentsOperationContext databaseContext, TransactionOperationContext indexContext, long? referenceCutoff, List<string> stalenessReasons)
        {
            return IsStaleDueToReferences(index, index._compiled, databaseContext, indexContext, referenceCutoff, stalenessReasons);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool IsStaleDueToReferences(MapReduceIndex index, DocumentsOperationContext databaseContext, TransactionOperationContext indexContext, long? referenceCutoff, List<string> stalenessReasons)
        {
            return IsStaleDueToReferences(index, index._compiled, databaseContext, indexContext, referenceCutoff, stalenessReasons);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static unsafe long CalculateIndexEtag(MapIndex index, int length, byte* indexEtagBytes, byte* writePos, DocumentsOperationContext documentsContext, TransactionOperationContext indexContext)
        {
            return CalculateIndexEtag(index, index._compiled, length, indexEtagBytes, writePos, documentsContext, indexContext);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static unsafe long CalculateIndexEtag(MapReduceIndex index, int length, byte* indexEtagBytes, byte* writePos, DocumentsOperationContext documentsContext, TransactionOperationContext indexContext)
        {
            return CalculateIndexEtag(index, index._compiled, length, indexEtagBytes, writePos, documentsContext, indexContext);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool IsStaleDueToReferences(Index index, StaticIndexBase compiled, DocumentsOperationContext databaseContext, TransactionOperationContext indexContext, long? referenceCutoff, List<string> stalenessReasons)
        {
            foreach (var collection in index.Collections)
            {
                if (compiled.ReferencedCollections.TryGetValue(collection, out HashSet<CollectionName> referencedCollections) == false)
                    continue;

                var lastIndexedEtag = index._indexStorage.ReadLastIndexedEtag(indexContext.Transaction, collection);
                // we haven't handled references for that collection yet
                // in theory we could check what is the last etag for that collection in documents store
                // but this was checked earlier by the base index class
                if (lastIndexedEtag == 0)
                    continue;

                foreach (var referencedCollection in referencedCollections)
                {
                    var lastDocEtag = databaseContext.DocumentDatabase.DocumentsStorage.GetLastDocumentEtag(databaseContext, referencedCollection.Name);
                    var lastProcessedReferenceEtag = index._indexStorage.ReadLastProcessedReferenceEtag(indexContext.Transaction, collection, referencedCollection);
                    var lastProcessedTombstoneEtag = index._indexStorage.ReadLastProcessedReferenceTombstoneEtag(indexContext.Transaction, collection, referencedCollection);

                    if (referenceCutoff == null)
                    {
                        if (lastDocEtag > lastProcessedReferenceEtag)
                        {
                            if (stalenessReasons == null)
                                return true;

                            var lastDoc = databaseContext.DocumentDatabase.DocumentsStorage.GetByEtag(databaseContext, lastDocEtag);

                            stalenessReasons.Add($"There are still some document references to process from collection '{referencedCollection.Name}'. " +
                                                 $"The last document etag in that collection is '{lastDocEtag:#,#;;0}' " +
                                                 $"({Constants.Documents.Metadata.Id}: '{lastDoc.Id}', " +
                                                 $"{Constants.Documents.Metadata.LastModified}: '{lastDoc.LastModified}'), " +
                                                 $"but last processed document etag for that collection is '{lastProcessedReferenceEtag:#,#;;0}'.");
                        }

                        var lastTombstoneEtag = databaseContext.DocumentDatabase.DocumentsStorage.GetLastTombstoneEtag(databaseContext, referencedCollection.Name);

                        if (lastTombstoneEtag > lastProcessedTombstoneEtag)
                        {
                            if (stalenessReasons == null)
                                return true;

                            var lastTombstone = databaseContext.DocumentDatabase.DocumentsStorage.GetTombstoneByEtag(databaseContext, lastTombstoneEtag);

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

                            var lastDoc = databaseContext.DocumentDatabase.DocumentsStorage.GetByEtag(databaseContext, lastDocEtag);

                            stalenessReasons.Add($"There are still some document references to process from collection '{referencedCollection.Name}'. " +
                                                 $"The last document etag in that collection is '{lastDocEtag:#,#;;0}' " +
                                                 $"({Constants.Documents.Metadata.Id}: '{lastDoc.Id}', " +
                                                 $"{Constants.Documents.Metadata.LastModified}: '{lastDoc.LastModified}') " +
                                                 $"with cutoff set to '{referenceCutoff.Value}', " +
                                                 $"but last processed document etag for that collection is '{lastProcessedReferenceEtag:#,#;;0}'.");
                        }

                        var hasTombstones = databaseContext.DocumentDatabase.DocumentsStorage.HasTombstonesWithEtagGreaterThanStartAndLowerThanOrEqualToEnd(databaseContext, referencedCollection.Name,
                            lastProcessedTombstoneEtag,
                            referenceCutoff.Value);
                        if (hasTombstones)
                        {
                            if (stalenessReasons == null)
                                return true;

                            stalenessReasons.Add($"There are still tomstones tombstones to process from collection '{referencedCollection.Name}' with etag range '{lastProcessedTombstoneEtag} - {referenceCutoff.Value}'.");
                        }
                    }
                }
            }

            return stalenessReasons?.Count > 0;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static unsafe long CalculateIndexEtag(Index index, StaticIndexBase compiled, int length, byte* indexEtagBytes, byte* writePos, DocumentsOperationContext documentsContext, TransactionOperationContext indexContext)
        {
            foreach (var collection in index.Collections)
            {
                if (compiled.ReferencedCollections.TryGetValue(collection, out HashSet<CollectionName> referencedCollections) == false)
                    continue;

                foreach (var referencedCollection in referencedCollections)
                {
                    var lastDocEtag = documentsContext.DocumentDatabase.DocumentsStorage.GetLastDocumentEtag(documentsContext, referencedCollection.Name);
                    var lastMappedEtag = index._indexStorage.ReadLastProcessedReferenceEtag(indexContext.Transaction, collection, referencedCollection);

                    *(long*)writePos = lastDocEtag;
                    writePos += sizeof(long);
                    *(long*)writePos = lastMappedEtag;
                }
            }

            unchecked
            {
                return (long)Hashing.XXHash64.Calculate(indexEtagBytes, (ulong)length);
            }
        }
    }
}
