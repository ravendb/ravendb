using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
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
        public static bool IsStale(MapIndex index, DocumentsOperationContext databaseContext, TransactionOperationContext indexContext, long? cutoff)
        {
            return IsStale(index, index._compiled, databaseContext, indexContext, cutoff);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool IsStale(MapReduceIndex index, DocumentsOperationContext databaseContext, TransactionOperationContext indexContext, long? cutoff)
        {
            return IsStale(index, index.Compiled, databaseContext, indexContext, cutoff);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static unsafe long CalculateIndexEtag(MapIndex index, int length, byte* indexEtagBytes, byte* writePos, DocumentsOperationContext documentsContext, TransactionOperationContext indexContext)
        {
            return CalculateIndexEtag(index, index._compiled, length, indexEtagBytes, writePos, documentsContext, indexContext);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static unsafe long CalculateIndexEtag(MapReduceIndex index, int length, byte* indexEtagBytes, byte* writePos, DocumentsOperationContext documentsContext, TransactionOperationContext indexContext)
        {
            return CalculateIndexEtag(index, index.Compiled, length, indexEtagBytes, writePos, documentsContext, indexContext);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool IsStale(Index index, StaticIndexBase compiled, DocumentsOperationContext databaseContext, TransactionOperationContext indexContext, long? cutoff)
        {
            foreach (var collection in index.Collections)
            {
                HashSet<CollectionName> referencedCollections;
                if (compiled.ReferencedCollections.TryGetValue(collection, out referencedCollections) == false)
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
                            return true;

                        var lastTombstoneEtag = databaseContext.DocumentDatabase.DocumentsStorage.GetLastTombstoneEtag(databaseContext, referencedCollection.Name);
                        var lastProcessedTombstoneEtag = index._indexStorage.ReadLastProcessedReferenceTombstoneEtag(indexContext.Transaction, collection, referencedCollection);

                        if (lastTombstoneEtag > lastProcessedTombstoneEtag)
                            return true;
                    }
                    else
                    {
                        if (Math.Min(cutoff.Value, lastDocEtag) > lastProcessedReferenceEtag)
                            return true;

                        if (databaseContext.DocumentDatabase.DocumentsStorage.GetNumberOfTombstonesWithDocumentEtagLowerThan(databaseContext, referencedCollection.Name, cutoff.Value) > 0)
                            return true;
                    }
                }
            }

            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static unsafe long CalculateIndexEtag(Index index, StaticIndexBase compiled, int length, byte* indexEtagBytes, byte* writePos, DocumentsOperationContext documentsContext, TransactionOperationContext indexContext)
        {
            foreach (var collection in index.Collections)
            {
                HashSet<CollectionName> referencedCollections;
                if (compiled.ReferencedCollections.TryGetValue(collection, out referencedCollections) == false)
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