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
        public static bool CanReplace(MapIndex index, bool isStale, DocumentDatabase database, DocumentsOperationContext databaseContext, TransactionOperationContext indexContext)
        {
            return isStale == false;
        }

        public static bool CanReplace(MapReduceIndex index, bool isStale, DocumentDatabase database, DocumentsOperationContext databaseContext, TransactionOperationContext indexContext)
        {
            return isStale == false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool IsStale(MapIndex index, DocumentsOperationContext databaseContext, TransactionOperationContext indexContext, long? cutoff, List<string> stalenessReasons)
        {
            return IsStale(index, index._compiled, databaseContext, indexContext, cutoff, stalenessReasons);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool IsStale(MapReduceIndex index, DocumentsOperationContext databaseContext, TransactionOperationContext indexContext, long? cutoff, List<string> stalenessReasons)
        {
            return IsStale(index, index._compiled, databaseContext, indexContext, cutoff, stalenessReasons);
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
        private static bool IsStale(Index index, StaticIndexBase compiled, DocumentsOperationContext databaseContext, TransactionOperationContext indexContext, long? cutoff, List<string> stalenessReasons)
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

                    if (cutoff == null)
                    {
                        if (lastDocEtag > lastProcessedReferenceEtag)
                        {
                            if (stalenessReasons == null)
                                return true;

                            var lastDoc = databaseContext.DocumentDatabase.DocumentsStorage.GetByEtag(databaseContext, lastDocEtag);

                            stalenessReasons.Add($"There are still some document references to process from collection '{referencedCollection}'. The last document etag in that collection is '{lastDocEtag}' ({Constants.Documents.Metadata.Id}: '{lastDoc.Id}', {Constants.Documents.Metadata.LastModified}: '{lastDoc.LastModified}'), but last processed document etag for that collection is '{lastProcessedReferenceEtag}'.");
                        }

                        var lastTombstoneEtag = databaseContext.DocumentDatabase.DocumentsStorage.GetLastTombstoneEtag(databaseContext, referencedCollection.Name);
                        var lastProcessedTombstoneEtag = index._indexStorage.ReadLastProcessedReferenceTombstoneEtag(indexContext.Transaction, collection, referencedCollection);

                        if (lastTombstoneEtag > lastProcessedTombstoneEtag)
                        {
                            if (stalenessReasons == null)
                                return true;

                            var lastTombstone = databaseContext.DocumentDatabase.DocumentsStorage.GetTombstoneByEtag(databaseContext, lastTombstoneEtag);

                            stalenessReasons.Add($"There are still some tombstone references to process from collection '{referencedCollection}'. The last tombstone etag in that collection is '{lastTombstoneEtag}' ({Constants.Documents.Metadata.Id}: '{lastTombstone.LowerId}', {Constants.Documents.Metadata.LastModified}: '{lastTombstone.LastModified}'), but last processed tombstone etag for that collection is '{lastProcessedTombstoneEtag}'.");
                        }
                    }
                    else
                    {
                        var minDocEtag = Math.Min(cutoff.Value, lastDocEtag);
                        if (minDocEtag > lastProcessedReferenceEtag)
                        {
                            if (stalenessReasons == null)
                                return true;

                            var lastDoc = databaseContext.DocumentDatabase.DocumentsStorage.GetByEtag(databaseContext, lastDocEtag);

                            stalenessReasons.Add($"There are still some document references to process from collection '{referencedCollection}'. The last document etag in that collection is '{lastDocEtag}' ({Constants.Documents.Metadata.Id}: '{lastDoc.Id}', {Constants.Documents.Metadata.LastModified}: '{lastDoc.LastModified}') with cutoff set to '{cutoff.Value}', but last processed document etag for that collection is '{lastProcessedReferenceEtag}'.");
                        }

                        var numberOfTombstones = databaseContext.DocumentDatabase.DocumentsStorage.GetNumberOfTombstonesWithDocumentEtagLowerThan(databaseContext, referencedCollection.Name, cutoff.Value);
                        if (numberOfTombstones > 0)
                        {
                            if (stalenessReasons == null)
                                return true;

                            stalenessReasons.Add($"There are still '{numberOfTombstones}' tombstone references to process from collection '{referencedCollection}' with document etag lower than '{cutoff.Value}'.");
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
