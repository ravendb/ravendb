using System;
using System.Collections.Generic;
using Raven.Server.Documents.Replication.ReplicationItems;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Binary;
using Sparrow.Server;
using Sparrow.Utils;
using Voron;
using Voron.Data.Tables;
using static Raven.Server.Documents.AttachmentsStorage;
using static Raven.Server.Documents.ConflictsStorage;
using static Raven.Server.Documents.CountersStorage;
using static Raven.Server.Documents.Revisions.RevisionsStorage;
using static Raven.Server.Documents.Schemas.Attachments;
using static Raven.Server.Documents.Schemas.Conflicts;
using static Raven.Server.Documents.Schemas.Counters;
using static Raven.Server.Documents.Schemas.DeletedRanges;
using static Raven.Server.Documents.Schemas.Documents;
using static Raven.Server.Documents.Schemas.Tombstones;
using static Raven.Server.Documents.Schemas.Revisions;
using static Raven.Server.Documents.Schemas.TimeSeries;
using static Raven.Server.Documents.TimeSeries.TimeSeriesStorage;

namespace Raven.Server.Documents.Sharding;

public unsafe class ShardedDocumentsStorage : DocumentsStorage
{
    private readonly ShardedDocumentDatabase _documentDatabase;

    public ShardedDocumentsStorage(ShardedDocumentDatabase documentDatabase, Action<string> addToInitLog) 
        : base(documentDatabase, addToInitLog)
    {
        _documentDatabase = documentDatabase;
    }

    protected override DocumentPutAction CreateDocumentPutAction()
    {
        return new ShardedDocumentPutAction(this, _documentDatabase);
    }
    
    public IEnumerable<Document> GetDocumentsByBucketFrom(DocumentsOperationContext context, int bucket, long etag)
    {
        var table = new Table(DocsSchema, context.Transaction.InnerTransaction);

        foreach (var result in GetItemsByBucket(context.Allocator, table, DocsSchema.DynamicKeyIndexes[AllDocsBucketAndEtagSlice], bucket, etag))
        {
            yield return TableValueToDocument(context, ref result.Result.Reader);
        }
    }

    public class BucketStatistics
    {
        public int Bucket;

        public long Size;

        public long NumberOfItems;

        public DateTime LastAccessed;
    }


    internal static void UpdateBucketStats_old(Transaction tx, Slice key, int oldSize, int newSize)
    {
        if (tx.IsWriteTransaction == false)
            return; // todo 

        var nowTicks = DateTime.UtcNow.Ticks;
        var bucket = Bits.SwapBytes(*(int*)key.Content.Ptr);

        if (tx.BucketStatistics.TryGetValue(bucket, out var bucketStats) == false)
        {
            var bucketStatsTree = tx.ReadTree(BucketStatsSlice);

            using (tx.Allocator.Allocate(sizeof(int), out var keyBuffer))
            using (tx.Allocator.Allocate(sizeof(long) * 3, out var statsBuffer))
            {
                *(int*)keyBuffer.Ptr = bucket;
                var keySlice = new Slice(keyBuffer);

                var readResult = bucketStatsTree.Read(keySlice);
                if (readResult != null)
                {
                    // we only need to read 'Size' and 'NumberOfItems', 'LastAccessedTicks' will be overriden  
                    readResult.Reader.Read(statsBuffer.Ptr, sizeof(long) * 2);
                }
                else
                {
                    *(long*)statsBuffer.Ptr = 0; // size
                    *(long*)(statsBuffer.Ptr + sizeof(long)) = 0; // number of items
                }

                bucketStats = *(Voron.Data.BucketStats*)statsBuffer.Ptr;
            }
        }

        bucketStats.Size += (newSize - oldSize);

        if (oldSize == 0)
            bucketStats.NumberOfItems++;
        else if (newSize == 0)
            bucketStats.NumberOfItems--;

        bucketStats.LastAccessedTicks = nowTicks;

        tx.BucketStatistics[bucket] = bucketStats;
    }

    internal static void UpdateBucketStats(Transaction tx, Slice key, int oldSize, int newSize)
    {
        if (tx.IsWriteTransaction == false)
            return; // todo 

        var nowTicks = DateTime.UtcNow.Ticks;
        var bucket = Bits.SwapBytes(*(int*)key.Content.Ptr);

        if (tx.BucketStatistics.TryGetValue(bucket, out var bucketStats) == false)
        {
            var bucketStatsTree = tx.ReadTree(BucketStatsSlice);

            using (tx.Allocator.Allocate(sizeof(int), out var keyBuffer))
            {
                *(int*)keyBuffer.Ptr = bucket;
                var keySlice = new Slice(keyBuffer);

                var readResult = bucketStatsTree.Read(keySlice);
                if (readResult != null)
                    bucketStats = *(Voron.Data.BucketStats*)readResult.Reader.Base;
            }
        }

        bucketStats.Size += newSize - oldSize;

        if (oldSize == 0)
            bucketStats.NumberOfItems++;
        else if (newSize == 0)
            bucketStats.NumberOfItems--;

        bucketStats.LastAccessedTicks = nowTicks;

        tx.BucketStatistics[bucket] = bucketStats;
    }

    public static BucketStatistics GetBucketStatistics(DocumentsOperationContext context, int bucket)
    {
        // todo throw if no read tx open 

        var tree = context.Transaction.InnerTransaction.ReadTree(BucketStatsSlice);

        using (context.Transaction.InnerTransaction.Allocator.Allocate(sizeof(int), out var keyBuffer))
        {
            *(int*)keyBuffer.Ptr = bucket;
            var readResult = tree.Read(new Slice(keyBuffer));
            if (readResult == null)
                return null;

            var stats = *(Voron.Data.BucketStats*)readResult.Reader.Base;

            return new BucketStatistics
            {
                Bucket = bucket,
                Size = stats.Size,
                NumberOfItems = stats.NumberOfItems,
                LastAccessed = new DateTime(stats.LastAccessedTicks)
            };
        }
    }
    public ChangeVector GetLastChangeVectorInBucket(DocumentsOperationContext context, int bucket)
    {
        var table = new Table(DocsSchema, context.Transaction.InnerTransaction);
        using (GetBucketAndEtagByteString(context.Allocator, bucket, long.MaxValue, out var buffer))
        using (Slice.External(context.Allocator, buffer, buffer.Length, out var keySlice))
        using (Slice.External(context.Allocator, buffer, buffer.Length - sizeof(long), out var prefix))
        {
            var result = table.SeekOneBackwardFrom(DocsSchema.DynamicKeyIndexes[AllDocsBucketAndEtagSlice], prefix, keySlice);
            if (result == null)
                return null;

            var document = TableValueToDocument(context, ref result.Reader, DocumentFields.ChangeVector);
            return context.GetChangeVector(document.ChangeVector);
        }
    }

    public ChangeVector GetMergedChangeVectorInBucket(DocumentsOperationContext context, int bucket)
    {
        DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Karmel, DevelopmentHelper.Severity.Normal,
            "Optimize this to calculate the merged change vector during insertion to the bucket");

        var table = new Table(DocsSchema, context.Transaction.InnerTransaction);
        var merged = context.GetChangeVector(string.Empty);

        if (GetLastItemInBucket(context.Allocator, table, DocsSchema.DynamicKeyIndexes[AllDocsBucketAndEtagSlice], bucket, out var reader))
        {
            var lastDocCv = TableValueToChangeVector(context, (int)DocumentsTable.ChangeVector, ref reader);
            merged = merged.MergeWith(lastDocCv, context);
        }
        if (GetLastItemInBucket(context.Allocator, table, TombstonesSchema.DynamicKeyIndexes[TombstonesBucketAndEtagSlice], bucket, out reader))
        {
            var lastTombstoneCv = TableValueToChangeVector(context, (int)TombstoneTable.ChangeVector, ref reader);
            merged = merged.MergeWith(lastTombstoneCv, context);
        }
        if (GetLastItemInBucket(context.Allocator, table, CountersSchema.DynamicKeyIndexes[CountersBucketAndEtagSlice], bucket, out reader))
        {
            var lastCounterCv = TableValueToChangeVector(context, (int)CountersTable.ChangeVector, ref reader);
            merged = merged.MergeWith(lastCounterCv, context);
        }
        if (GetLastItemInBucket(context.Allocator, table, ConflictsSchema.DynamicKeyIndexes[ConflictsBucketAndEtagSlice], bucket, out reader))
        {
            var lastConflictCv = TableValueToChangeVector(context, (int)ConflictsTable.ChangeVector, ref reader);
            merged = merged.MergeWith(lastConflictCv, context);
        }
        if (GetLastItemInBucket(context.Allocator, table, RevisionsSchema.DynamicKeyIndexes[RevisionsBucketAndEtagSlice], bucket, out reader))
        {
            var lastRevisionCv = TableValueToChangeVector(context, (int)RevisionsTable.ChangeVector, ref reader);
            merged = merged.MergeWith(lastRevisionCv, context);
        }
        if (GetLastItemInBucket(context.Allocator, table, AttachmentsSchema.DynamicKeyIndexes[AttachmentsBucketAndEtagSlice], bucket, out reader))
        {
            var lastAttachmentCv = TableValueToChangeVector(context, (int)AttachmentsTable.ChangeVector, ref reader);
            merged = merged.MergeWith(lastAttachmentCv, context);
}
        if (GetLastItemInBucket(context.Allocator, table, TimeSeriesSchema.DynamicKeyIndexes[TimeSeriesBucketAndEtagSlice], bucket, out reader))
        {
            var lastTimeSeriesCv = TableValueToChangeVector(context, (int)TimeSeriesTable.ChangeVector, ref reader);
            merged = merged.MergeWith(lastTimeSeriesCv, context);
        }
        if (GetLastItemInBucket(context.Allocator, table, DeleteRangesSchema.DynamicKeyIndexes[DeletedRangesBucketAndEtagSlice], bucket, out reader))
        {
            var lastDeleteRangeCv = TableValueToChangeVector(context, (int)DeletedRangeTable.ChangeVector, ref reader);
            merged = merged.MergeWith(lastDeleteRangeCv, context);
        }

        return merged;
    }

    public bool HaveMoreDocumentsInBucket(int bucket, string current)
    {
        using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        using (context.OpenReadTransaction())
        {
            var last = GetLastChangeVectorInBucket(context, bucket);
            if (last == null)
                return false;

            var status = ChangeVector.GetConflictStatusForDatabase(last, context.GetChangeVector(current));
            if (status == ConflictStatus.AlreadyMerged)
                return false;

            return true;
        }
    }

    public IEnumerable<ReplicationBatchItem> GetTombstonesByBucketFrom(DocumentsOperationContext context, int bucket, long etag)
    {
        var table = new Table(TombstonesSchema, context.Transaction.InnerTransaction);

        foreach (var result in GetItemsByBucket(context.Allocator, table, TombstonesSchema.DynamicKeyIndexes[TombstonesBucketAndEtagSlice], bucket, etag))
        {
            yield return TombstoneReplicationItem.From(context, TableValueToTombstone(context, ref result.Result.Reader));
        }
    }

    public static IEnumerable<Table.SeekResult> GetItemsByBucket(ByteStringContext allocator, Table table,
        TableSchema.DynamicKeyIndexDef dynamicIndex, int bucket, long etag)
    {
        using (GetBucketAndEtagByteString(allocator, bucket, etag, out var buffer))
        using (Slice.External(allocator, buffer, buffer.Length, out var keySlice))
        using (Slice.External(allocator, buffer, buffer.Length - sizeof(long), out var prefix))
        {
            foreach (var result in table.SeekForwardFromPrefix(dynamicIndex, keySlice, prefix, 0))
            {
                yield return result;
            }
        }
    }

    public static bool GetLastItemInBucket(ByteStringContext allocator, Table table,
        TableSchema.DynamicKeyIndexDef dynamicIndex, int bucket, out TableValueReader reader)
    {
        reader = default;
        using (GetBucketAndEtagByteString(allocator, bucket, etag : long.MaxValue, out var buffer))
        using (Slice.External(allocator, buffer, buffer.Length, out var keySlice))
        using (Slice.External(allocator, buffer, buffer.Length - sizeof(long), out var prefix))
        {
            var holder = table.SeekOneBackwardFrom(dynamicIndex, prefix, keySlice);
            if (holder == null)
                return false;

            reader = holder.Reader;
            return true;
        }
    }

    public const long MaxDocumentsToDeleteInBucket = 1024;

    public long DeleteBucket(DocumentsOperationContext context, int bucket, ChangeVector upTo)
    {
        long numOfDeletions = 0;

        MarkTombstonesAsArtificial(context, bucket);

        foreach (var document in GetDocumentsByBucketFrom(context, bucket, 0))
        {
            if (numOfDeletions > MaxDocumentsToDeleteInBucket)
                break;

            var docCv = context.GetChangeVector(document.ChangeVector);
            if (ChangeVectorUtils.GetConflictStatus(docCv, upTo) != ConflictStatus.AlreadyMerged)
                break;

            // check change vectors of all document extensions
            if (HasDocumentExtensionWithGreaterChangeVector(context, document.LowerId, upTo)) 
                break;
            
            Delete(context, document.Id, flags: DocumentFlags.Artificial | DocumentFlags.FromResharding);

            // delete revisions for document
            RevisionsStorage.DeleteRevisionsFor(context, document.Id, flags: DocumentFlags.Artificial | DocumentFlags.FromResharding);

            numOfDeletions++;
        }

        return numOfDeletions;
    }

    private void MarkTombstonesAsArtificial(DocumentsOperationContext context, int bucket)
    {
        long lastProcessedEtag = 0;
        bool hasMore = true, collectionNamesUpdated = false;
        var collectionNames = new Dictionary<string, CollectionName>(_collectionsCache, StringComparer.OrdinalIgnoreCase);
        var readTable = new Table(TombstonesSchema, context.Transaction.InnerTransaction);

        while (hasMore)
        {
            hasMore = false;
            foreach (var result in GetItemsByBucket(context.Allocator, readTable, TombstonesSchema.DynamicKeyIndexes[TombstonesBucketAndEtagSlice], bucket, etag: lastProcessedEtag))
            {
                var tombstone = TableValueToTombstone(context, ref result.Result.Reader);
                if (tombstone.Flags.Contain(DocumentFlags.Artificial) && tombstone.Flags.Contain(DocumentFlags.FromResharding))
                    continue;

                var collection = TableValueToId(context, (int)TombstoneTable.Collection, ref result.Result.Reader);
                if (collectionNames.TryGetValue(collection, out var collectionName) == false)
                {
                    collectionNames[collection] = collectionName = new CollectionName(collection);
                    collectionNamesUpdated = true;
                }

                var writeTable = context.Transaction.InnerTransaction.OpenTable(TombstonesSchema, collectionName.GetTableName(CollectionTableType.Tombstones));

                var newEtag = GenerateNextEtag();
                var cv = ChangeVector.Merge(context.LastDatabaseChangeVector, context.GetChangeVector(tombstone.ChangeVector), context);
                var flags = tombstone.Flags | DocumentFlags.Artificial | DocumentFlags.FromResharding;

                using (Slice.From(context.Allocator, cv, out var cvSlice))
                using (Slice.External(context.Allocator, tombstone.LowerId, out var keySlice))
                using (DocumentIdWorker.GetStringPreserveCase(context, collection, out Slice collectionSlice))
                using (writeTable.Allocate(out TableValueBuilder tvb))
                {
                    var clonedKey = keySlice.Clone(context.Allocator);

                    tvb.Add(clonedKey.Content.Ptr, clonedKey.Size);
                    tvb.Add(Bits.SwapBytes(newEtag));
                    tvb.Add(Bits.SwapBytes(tombstone.DeletedEtag));
                    tvb.Add(tombstone.TransactionMarker);
                    tvb.Add((byte)tombstone.Type);
                    tvb.Add(collectionSlice);
                    tvb.Add((int)flags);
                    tvb.Add(cvSlice.Content.Ptr, cvSlice.Size);
                    tvb.Add(tombstone.LastModified.Ticks);

                    writeTable.Update(tombstone.StorageId, tvb);
                    context.Allocator.Release(ref clonedKey.Content);
                }

                // need to re open the read iterator after we modified the tree
                lastProcessedEtag = tombstone.Etag;
                hasMore = true;
                break;
            }
        }

        if (collectionNamesUpdated)
        {
            // Add to cache ONLY if the transaction was committed.
            // this would prevent NREs next time a PUT is run,since if a transaction
            // is not committed, DocsSchema and TombstonesSchema will not be actually created..
            // has to happen after the commit, but while we are holding the write tx lock
            context.Transaction.InnerTransaction.LowLevelTransaction.BeforeCommitFinalization += _ =>
            {
                _collectionsCache = collectionNames;
            };
        }

    }

    private bool HasDocumentExtensionWithGreaterChangeVector(DocumentsOperationContext context, string documentId, ChangeVector upTo)
    {
        var counters = CountersStorage.GetCounterValuesForDocument(context, documentId);
        foreach (var counter in counters)
        {
            var counterCv = context.GetChangeVector(counter.ChangeVector);
            if (ChangeVectorUtils.GetConflictStatus(counterCv, upTo) == ConflictStatus.Update)
                return true;
        }

        foreach (var ts in TimeSeriesStorage.GetTimeSeriesNamesForDocument(context, documentId))
        {
            var segments = TimeSeriesStorage.GetSegmentsSummary(context, documentId, ts.ToString(), DateTime.MinValue, DateTime.MaxValue);
            foreach (var segment in segments)
            {
                var tsCv = context.GetChangeVector(segment.ChangeVector);
                if (ChangeVectorUtils.GetConflictStatus(tsCv, upTo) == ConflictStatus.Update)
                    return true;
            }
        }

        using (DocumentIdWorker.GetLowerIdSliceAndStorageKey(context, documentId, out var lowerId, out _))
        {
            var attachments = AttachmentsStorage.GetAttachmentDetailsForDocument(context, lowerId);
            foreach (var attachment in attachments)
            {
                var attachmentCv = context.GetChangeVector(attachment.ChangeVector);
                if (ChangeVectorUtils.GetConflictStatus(attachmentCv, upTo) == ConflictStatus.Update)
                    return true;
            }
        }

        var revisions = RevisionsStorage.GetRevisions(context, documentId, 0, int.MaxValue).Revisions;
        foreach (var revision in revisions)
        {
            var revisionCv = context.GetChangeVector(revision.ChangeVector);
            if (ChangeVectorUtils.GetConflictStatus(revisionCv, upTo) == ConflictStatus.Update)
                return true;
        }

        return false;
    }

    public static ByteStringContext<ByteStringMemoryCache>.InternalScope GetBucketByteString(
        ByteStringContext allocator, int bucket,
        out ByteString buffer)
    {
        var scope = allocator.Allocate(sizeof(int), out buffer);
        *(int*)buffer.Ptr = Bits.SwapBytes(bucket);

        return scope;
    }

    public static ByteStringContext<ByteStringMemoryCache>.InternalScope GetBucketAndEtagByteString(
        ByteStringContext allocator, int bucket, long etag,
        out ByteString buffer)
    {
        var scope = allocator.Allocate(sizeof(int) + sizeof(long), out buffer);
        *(int*)buffer.Ptr = Bits.SwapBytes(bucket);
        *(long*)(buffer.Ptr + sizeof(int)) = Bits.SwapBytes(etag);

        return scope;
    }
}
