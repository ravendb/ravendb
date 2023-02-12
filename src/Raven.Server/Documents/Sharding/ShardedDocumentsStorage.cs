using System;
using System.Collections.Generic;
using System.Text;
using Raven.Server.Documents.Replication.ReplicationItems;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Binary;
using Sparrow.Server;
using Sparrow.Server.Utils;
using Sparrow.Utils;
using Voron;
using Voron.Data.Tables;
using Voron.Impl;
using static Raven.Server.Documents.Schemas.Attachments;
using static Raven.Server.Documents.Schemas.Conflicts;
using static Raven.Server.Documents.Schemas.Counters;
using static Raven.Server.Documents.Schemas.DeletedRanges;
using static Raven.Server.Documents.Schemas.Documents;
using static Raven.Server.Documents.Schemas.Revisions;
using static Raven.Server.Documents.Schemas.TimeSeries;
using static Raven.Server.Documents.Schemas.Tombstones;

namespace Raven.Server.Documents.Sharding;

public unsafe class ShardedDocumentsStorage : DocumentsStorage
{
    public static readonly Slice BucketStatsSlice;

    internal Dictionary<int, Documents.BucketStats> BucketStatistics => _bucketStatistics ??= new Dictionary<int, Documents.BucketStats>();

    private Dictionary<int, Documents.BucketStats> _bucketStatistics;
    private readonly ShardedDocumentDatabase _documentDatabase;

    static ShardedDocumentsStorage()
    {
        using (StorageEnvironment.GetStaticContext(out var ctx))
        {
            Slice.From(ctx, "BucketStats", ByteStringType.Immutable, out BucketStatsSlice);
        }
    }

    public ShardedDocumentsStorage(ShardedDocumentDatabase documentDatabase, Action<string> addToInitLog) 
        : base(documentDatabase, addToInitLog)
    {
        _documentDatabase = documentDatabase;
        OnBeforeCommit += UpdateBucketStatsTreeBeforeCommit;
    }

    protected override DocumentPutAction CreateDocumentPutAction()
    {
        return new ShardedDocumentPutAction(this, _documentDatabase);
    }

    protected override void SetDocumentsStorageSchemas()
    {
        DocsSchema = Schemas.Documents.ShardingDocsSchemaBase;
        TombstonesSchema = Schemas.Tombstones.ShardingTombstonesSchema;
        CompressedDocsSchema = Schemas.Documents.ShardingCompressedDocsSchemaBase;

        AttachmentsSchema = Schemas.Attachments.ShardingAttachmentsSchemaBase;
        ConflictsSchema = Schemas.Conflicts.ShardingConflictsSchemaBase;
        CountersSchema = Schemas.Counters.ShardingCountersSchemaBase;

        TimeSeriesSchema = Schemas.TimeSeries.ShardingTimeSeriesSchemaBase;
        TimeSeriesDeleteRangesSchema = Schemas.DeletedRanges.ShardingDeleteRangesSchemaBase;

        RevisionsSchema = Schemas.Revisions.ShardingRevisionsSchemaBase;
        CompressedRevisionsSchema = Schemas.Revisions.ShardingCompressedRevisionsSchemaBase;
    }

    public IEnumerable<Document> GetDocumentsByBucketFrom(DocumentsOperationContext context, int bucket, long etag)
    {
        var table = new Table(DocsSchema, context.Transaction.InnerTransaction);

        foreach (var result in GetItemsByBucket(context.Allocator, table, DocsSchema.DynamicKeyIndexes[AllDocsBucketAndEtagSlice], bucket, etag))
        {
            yield return TableValueToDocument(context, ref result.Result.Reader);
        }
    }

    public static IEnumerable<BucketStats> GetBucketStatistics(DocumentsOperationContext context, int fromBucket, int toBucket)
    {
        var tree = context.Transaction.InnerTransaction.ReadTree(BucketStatsSlice);
        if (tree == null)
            yield break;

        using (GetBucketByteString(context.Allocator, fromBucket, out var buffer))
        using (Slice.External(context.Allocator, buffer, buffer.Length, out var startSlice))
        {
            using (var it = tree.Iterate(prefetch: true))
            {
                if (it.Seek(startSlice) == false)
                    yield break;

                do
                {
                    var bucket = GetBucketNumberFromBucketStatsKey(it.CurrentKey);
                    if (bucket > toBucket)
                        yield break;

                    var reader = it.CreateReaderForCurrent();
                    yield return ValueReaderToBucketStats(bucket, ref reader);

                } while (it.MoveNext());
            }
        }
    }

    public static BucketStats GetBucketStatisticsFor(DocumentsOperationContext context, int bucket)
    {
        var tree = context.Transaction.InnerTransaction.ReadTree(BucketStatsSlice);

        using (context.Transaction.InnerTransaction.Allocator.Allocate(sizeof(int), out var keyBuffer))
        {
            *(int*)keyBuffer.Ptr = Bits.SwapBytes(bucket);
            var readResult = tree.Read(new Slice(keyBuffer));
            if (readResult == null)
                return null;

            var reader = readResult.Reader;
            return ValueReaderToBucketStats(bucket, ref reader);
        }
    }

    private static BucketStats ValueReaderToBucketStats(int bucket, ref ValueReader valueReader)
    {
        var stats = *(Documents.BucketStats*)valueReader.Base;
        return new BucketStats
        {
            Bucket = bucket,
            Size = stats.Size,
            NumberOfDocuments = stats.NumberOfDocuments,
            LastModified = new DateTime(stats.LastModifiedTicks, DateTimeKind.Utc)
        };
    }

    private static int GetBucketNumberFromBucketStatsKey(Slice key)
    {
        return Bits.SwapBytes(*(int*)key.Content.Ptr);
    }

    [StorageIndexEntryKeyGenerator]
    internal static ByteStringContext.Scope GenerateBucketAndEtagIndexKeyForDocuments(Transaction tx, ref TableValueReader tvr, out Slice slice)
    {
        return GenerateBucketAndEtagIndexKey(tx, idIndex: (int)DocumentsTable.LowerId, etagIndex: (int)DocumentsTable.Etag, ref tvr, out slice);
    }

    [StorageIndexEntryKeyGenerator]
    internal static ByteStringContext.Scope GenerateBucketAndEtagIndexKeyForTombstones(Transaction tx, ref TableValueReader tvr, out Slice slice)
    {
        return GenerateBucketAndEtagIndexKey(tx, idIndex: (int)TombstoneTable.LowerId, etagIndex: (int)TombstoneTable.Etag, ref tvr, out slice);
    }

    internal static ByteStringContext.Scope GenerateBucketAndEtagIndexKey(Transaction tx, int idIndex, int etagIndex, ref TableValueReader tvr, out Slice slice)
    {
        var lowerId = tvr.Read(idIndex, out int size);
        size = UnwrapLowerIdIfNeeded(lowerId, size);
        var etag = *(long*)tvr.Read(etagIndex, out _);
        return GenerateBucketAndEtagSlice(tx, lowerId, size, etag, out slice);
    }

    internal static ByteStringContext.Scope ExtractIdFromKeyAndGenerateBucketAndEtagIndexKey(Transaction tx, int keyIndex, int etagIndex, ref TableValueReader tvr, out Slice slice)
    {
        var keyPtr = tvr.Read(keyIndex, out var keySize);

        int sizeOfDocId = 0;
        for (; sizeOfDocId < keySize; sizeOfDocId++)
        {
            if (keyPtr[sizeOfDocId] == SpecialChars.RecordSeparator)
                break;
        }

        var etag = *(long*)tvr.Read(etagIndex, out _);

        return GenerateBucketAndEtagSlice(tx, keyPtr, sizeOfDocId, etag, out slice);
    }

    private static ByteStringContext.Scope GenerateBucketAndEtagSlice(Transaction tx, byte* idPtr, int idSize, long etag, out Slice slice)
    {
        var scope = tx.Allocator.Allocate(sizeof(long) + sizeof(int), out var buffer);

        var bucket = ShardHelper.GetBucketFor(idPtr, idSize);

        var database = tx.Owner as ShardedDocumentDatabase;
        var prefixedConfiguration = database?.ShardingConfiguration?.Prefixed;
        if (prefixedConfiguration is { Count: > 0 })
        {
            var idAsStr = Encoding.UTF8.GetString(idPtr, idSize);
            foreach (var setting in prefixedConfiguration)
            {
                var prefix = setting.Prefix;
                 
                if (idAsStr.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
                {
                    bucket += setting.BucketRangeStart;
                    break;
                }
            }
        }

        *(int*)buffer.Ptr = Bits.SwapBytes(bucket);
        *(long*)(buffer.Ptr + sizeof(int)) = etag;

        slice = new Slice(buffer);
        return scope;
    }

    internal static void UpdateBucketStatsForDocument(Transaction tx, Slice key, long oldSize, long newSize)
    {
        int numOfDocsChanged = 0;
        if (oldSize == 0)
            numOfDocsChanged = 1;
        else if (newSize == 0)
            numOfDocsChanged = -1;

        UpdateBucketStatsInternal(tx, key, oldSize, newSize, numOfDocsChanged);
    }

    internal static void UpdateBucketStats(Transaction tx, Slice key, long oldSize, long newSize)
    {
        UpdateBucketStatsInternal(tx, key, oldSize, newSize, numOfDocsChanged: 0);
    }

    private static void UpdateBucketStatsInternal(Transaction tx, Slice key, long oldSize, long newSize, int numOfDocsChanged)
    {
        if (tx.Owner is not ShardedDocumentDatabase documentDatabase)
            return;

        var nowTicks = documentDatabase.Time.GetUtcNow().Ticks;
        var bucket = *(int*)key.Content.Ptr;

        var inMemoryBucketStats = documentDatabase.ShardedDocumentsStorage.BucketStatistics;
        inMemoryBucketStats.TryGetValue(bucket, out var bucketStats);

        bucketStats.Size += newSize - oldSize;
        bucketStats.NumberOfDocuments += numOfDocsChanged;
        bucketStats.LastModifiedTicks = nowTicks;

        inMemoryBucketStats[bucket] = bucketStats;
    }

    internal void UpdateBucketStatsTreeBeforeCommit(Transaction tx)
    {
        if (_bucketStatistics == null)
            return;

        var tree = tx.ReadTree(BucketStatsSlice);
        foreach ((int bucket, Documents.BucketStats inMemoryStats) in _bucketStatistics)
        {
            using (tx.Allocator.Allocate(sizeof(int), out var keyBuffer))
            {
                *(int*)keyBuffer.Ptr = bucket;
                var keySlice = new Slice(keyBuffer);
                var readResult = tree.Read(keySlice);

                Documents.BucketStats stats;
                if (readResult == null)
                {
                    stats = inMemoryStats;
                }
                else
                {
                    stats = *(Documents.BucketStats*)readResult.Reader.Base;
                    stats.Size += inMemoryStats.Size;
                    stats.NumberOfDocuments += inMemoryStats.NumberOfDocuments;
                    stats.LastModifiedTicks = inMemoryStats.LastModifiedTicks;
                }

                if (stats.Size == 0 && stats.NumberOfDocuments == 0)
                {
                    tree.Delete(keySlice);
                    continue;
                }

                using (tree.DirectAdd(keySlice, sizeof(Documents.BucketStats), out byte* ptr))
                    *(Documents.BucketStats*)ptr = stats;
            }
        }

        _bucketStatistics = null;
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
        foreach (var result in GetItemsByBucket(context.Allocator, table, DocsSchema.DynamicKeyIndexes[AllDocsBucketAndEtagSlice], bucket, 0))
        {
            var documentCv = TableValueToChangeVector(context, (int)DocumentsTable.ChangeVector, ref result.Result.Reader);
            merged = merged.MergeWith(documentCv, context);
        }
        foreach (var result in GetItemsByBucket(context.Allocator, table, TombstonesSchema.DynamicKeyIndexes[TombstonesBucketAndEtagSlice], bucket, 0))
        {
            var tombstoneCv = TableValueToChangeVector(context, (int)TombstoneTable.ChangeVector, ref result.Result.Reader);
            var flags = TableValueToFlags((int)TombstoneTable.Flags, ref result.Result.Reader);
            if (flags.HasFlag(DocumentFlags.Artificial | DocumentFlags.FromResharding))
                continue;

            merged = merged.MergeWith(tombstoneCv, context);
        }
        foreach (var result in GetItemsByBucket(context.Allocator, table, _documentDatabase.DocumentsStorage.CountersStorage.CountersSchema.DynamicKeyIndexes[CountersBucketAndEtagSlice], bucket, 0))
        {
            var counterCv = TableValueToChangeVector(context, (int)CountersTable.ChangeVector, ref result.Result.Reader);
            merged = merged.MergeWith(counterCv, context);
        }
        foreach (var result in GetItemsByBucket(context.Allocator, table, _documentDatabase.DocumentsStorage.ConflictsStorage.ConflictsSchema.DynamicKeyIndexes[ConflictsBucketAndEtagSlice], bucket, 0))
        {
            var conflictCv = TableValueToChangeVector(context, (int)ConflictsTable.ChangeVector, ref result.Result.Reader);
            merged = merged.MergeWith(conflictCv, context);
        }
        foreach (var result in GetItemsByBucket(context.Allocator, table, _documentDatabase.DocumentsStorage.RevisionsStorage.RevisionsSchema.DynamicKeyIndexes[RevisionsBucketAndEtagSlice], bucket, 0))
        {
            var revisionCv = TableValueToChangeVector(context, (int)RevisionsTable.ChangeVector, ref result.Result.Reader);
            merged = merged.MergeWith(revisionCv, context);
        }
        foreach (var result in GetItemsByBucket(context.Allocator, table, _documentDatabase.DocumentsStorage.AttachmentsStorage.AttachmentsSchema.DynamicKeyIndexes[AttachmentsBucketAndEtagSlice], bucket, 0))
        {
            var attachmentCv = TableValueToChangeVector(context, (int)AttachmentsTable.ChangeVector, ref result.Result.Reader);
            merged = merged.MergeWith(attachmentCv, context);
        }
        foreach (var result in GetItemsByBucket(context.Allocator, table, _documentDatabase.DocumentsStorage.TimeSeriesStorage.TimeSeriesSchema.DynamicKeyIndexes[TimeSeriesBucketAndEtagSlice], bucket, 0))
        {
            var tsCv = TableValueToChangeVector(context, (int)TimeSeriesTable.ChangeVector, ref result.Result.Reader);
            merged = merged.MergeWith(tsCv, context);
        }
        foreach (var result in GetItemsByBucket(context.Allocator, table, _documentDatabase.DocumentsStorage.TimeSeriesStorage.DeleteRangesSchema.DynamicKeyIndexes[DeletedRangesBucketAndEtagSlice], bucket, 0))
        {
            var deletedRangeCv = TableValueToChangeVector(context, (int)DeletedRangeTable.ChangeVector, ref result.Result.Reader);
            merged = merged.MergeWith(deletedRangeCv, context);
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

        MarkTombstonesAsArtificial(context, bucket, upTo);

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

    private void MarkTombstonesAsArtificial(DocumentsOperationContext context, int bucket, ChangeVector upTo)
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

                var tombstoneChangeVector = context.GetChangeVector(tombstone.ChangeVector);
                if (ChangeVectorUtils.GetConflictStatus(tombstoneChangeVector, upTo) != ConflictStatus.AlreadyMerged)
                    break;

                var collection = TableValueToId(context, (int)TombstoneTable.Collection, ref result.Result.Reader);
                if (collectionNames.TryGetValue(collection, out var collectionName) == false)
                {
                    collectionNames[collection] = collectionName = new CollectionName(collection);
                    collectionNamesUpdated = true;
                }

                var writeTable = context.Transaction.InnerTransaction.OpenTable(TombstonesSchema, collectionName.GetTableName(CollectionTableType.Tombstones));

                var newEtag = GenerateNextEtag();
                var cv = ChangeVector.Merge(context.LastDatabaseChangeVector, tombstoneChangeVector, context);
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
