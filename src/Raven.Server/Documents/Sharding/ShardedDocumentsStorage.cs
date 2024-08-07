using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Changes;
using Raven.Client.Exceptions.Sharding;
using Raven.Server.Documents.Replication.ReplicationItems;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Binary;
using Sparrow.Logging;
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

public sealed unsafe class ShardedDocumentsStorage : DocumentsStorage
{
    public static readonly Slice BucketStatsSlice;

    private readonly BucketStatsHolder _bucketStats;

    private readonly ShardedDocumentDatabase _documentDatabase;

    public Action<LowLevelTransaction> OnFailure { get; }

    static ShardedDocumentsStorage()
    {
        using (StorageEnvironment.GetStaticContext(out var ctx))
        {
            Slice.From(ctx, "BucketStats", ByteStringType.Immutable, out BucketStatsSlice);
        }
    }

    public ShardedDocumentsStorage(ShardedDocumentDatabase documentDatabase, Action<LogMode, string> addToInitLog)
        : base(documentDatabase, addToInitLog)
    {
        _documentDatabase = documentDatabase;
        _bucketStats = new BucketStatsHolder();

        OnFailure += _bucketStats.ClearBucketStatsOnFailure;
        OnBeforeCommit += _bucketStats.UpdateBucketStatsTreeBeforeCommit;
    }

    protected override DocumentPutAction CreateDocumentPutAction()
    {
        return new ShardedDocumentPutAction(this, _documentDatabase);
    }

    protected override void SetDocumentsStorageSchemas()
    {
        DocsSchema = ShardingDocsSchemaBase;
        TombstonesSchema = ShardingTombstonesSchema;
        CompressedDocsSchema = ShardingCompressedDocsSchemaBase;

        AttachmentsSchema = ShardingAttachmentsSchemaBase;
        ConflictsSchema = ShardingConflictsSchemaBase;
        CountersSchema = ShardingCountersSchemaBase;
        CounterTombstonesSchema = Schemas.CounterTombstones.ShardingCounterTombstonesSchemaBase;

        TimeSeriesSchema = ShardingTimeSeriesSchemaBase;
        TimeSeriesDeleteRangesSchema = ShardingDeleteRangesSchemaBase;

        RevisionsSchema = ShardingRevisionsSchemaBase;
        CompressedRevisionsSchema = ShardingCompressedRevisionsSchemaBase;
    }

    public IEnumerable<Document> GetDocumentsByBucketFrom(DocumentsOperationContext context, int bucket, long etag, long skip = 0, long take = long.MaxValue, DocumentFields fields = DocumentFields.All)
    {
        var table = new Table(DocsSchema, context.Transaction.InnerTransaction);

        foreach (var result in GetItemsByBucket(context.Allocator, table, DocsSchema.DynamicKeyIndexes[AllDocsBucketAndEtagSlice], bucket, etag, skip, take))
        {
            yield return TableValueToDocument(context, ref result.Result.Reader, fields);
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
        return ExtractIdFromKeyAndGenerateBucketAndEtagIndexKey(tx, (int)TombstoneTable.LowerId, etagIndex: (int)TombstoneTable.Etag, ref tvr, out slice);
    }

    internal static ByteStringContext.Scope GenerateBucketAndEtagIndexKey(Transaction tx, int idIndex, int etagIndex, ref TableValueReader tvr, out Slice slice)
    {
        var lowerId = tvr.Read(idIndex, out int size);
        size = GetSizeOfTombstoneId(lowerId, size);
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

        var span = new ReadOnlySpan<byte>(idPtr, idSize);
        
        var docsCtx = tx.Owner as DocumentsOperationContext;
        var database = ShardedDocumentDatabase.CastToShardedDocumentDatabase(docsCtx?.DocumentDatabase);
        var bucket = ShardHelper.GetBucketFor(database?.ShardingConfiguration, span);

        *(int*)buffer.Ptr = Bits.SwapBytes(bucket);
        *(long*)(buffer.Ptr + sizeof(int)) = etag;

        slice = new Slice(buffer);
        return scope;
    }

    internal static void UpdateBucketStatsForDocument(Transaction tx, Slice key, ref TableValueReader oldValue, ref TableValueReader newValue)
    {
        int numOfDocsChanged = 0;
        if (oldValue.Size == 0)
        {
            // a new document was inserted
            numOfDocsChanged = 1;
        }
        else if (newValue.Size == 0)
        {
            // a document was deleted
            numOfDocsChanged = -1;
        }

        UpdateBucketStatsInternal(tx, key, ref newValue, changeVectorIndex: (int)DocumentsTable.ChangeVector, sizeChange: newValue.Size - oldValue.Size, numOfDocsChanged);
    }

    internal static void UpdateBucketStatsForTombstones(Transaction tx, Slice key, ref TableValueReader oldValue, ref TableValueReader newValue)
    {
        if (newValue.Size > 0)
        {
            var flags = TableValueToFlags((int)TombstoneTable.Flags, ref newValue);
            if (flags.Contain(DocumentFlags.Artificial))
            {
                // we don't want to update the merged-cv of the bucket for artificial tombstones
                UpdateBucketStatsInternal(tx, key, sizeChange: newValue.Size - oldValue.Size);
                return;
            }
        }

        UpdateBucketStatsInternal(tx, key, ref newValue, changeVectorIndex: (int)TombstoneTable.ChangeVector, sizeChange: newValue.Size - oldValue.Size);
    }

    internal static void UpdateBucketStatsInternal(Transaction tx, Slice key, ref TableValueReader value, int changeVectorIndex, long sizeChange, int numOfDocsChanged = 0)
    {
        if (tx.Owner is not DocumentsOperationContext { DocumentDatabase: ShardedDocumentDatabase documentDatabase } context)
            return;

        var nowTicks = documentDatabase.Time.GetUtcNow().Ticks;
        var bucket = *(int*)key.Content.Ptr;
        var inMemoryBucketStats = documentDatabase.ShardedDocumentsStorage._bucketStats;

        if (value.Size == 0)
        {
            // item deletion 
            // no need to update the merged-cv
            inMemoryBucketStats.UpdateBucket(bucket, nowTicks, sizeChange, numOfDocsChanged);
            return;
        }

        // item was inserted/updated 
        // need to update the merged-cv of the bucket
        inMemoryBucketStats.UpdateBucketAndChangeVector(context, bucket, nowTicks, sizeChange, numOfDocsChanged, changeVectorIndex, ref value);
    }

    internal static void UpdateBucketStatsInternal(Transaction tx, Slice key, long sizeChange, int numOfDocsChanged = 0)
    {
        if (tx.Owner is not DocumentsOperationContext { DocumentDatabase: ShardedDocumentDatabase documentDatabase })
            return;

        var nowTicks = documentDatabase.Time.GetUtcNow().Ticks;
        var bucket = *(int*)key.Content.Ptr;
        var inMemoryBucketStats = documentDatabase.ShardedDocumentsStorage._bucketStats;

        inMemoryBucketStats.UpdateBucket(bucket, nowTicks, sizeChange, numOfDocsChanged);
    }

    public ChangeVector GetMergedChangeVectorInBucket(DocumentsOperationContext context, int bucket)
    {
        var tree = context.Transaction.InnerTransaction.ReadTree(BucketStatsSlice);

        using (context.Transaction.InnerTransaction.Allocator.Allocate(sizeof(int), out var keyBuffer))
        {
            *(int*)keyBuffer.Ptr = Bits.SwapBytes(bucket);
            var readResult = tree.Read(new Slice(keyBuffer));
            if (readResult == null)
                return null;

            var cvStr = Documents.BucketStats.GetMergedChangeVector(readResult.Reader);
            return context.GetChangeVector(cvStr);
        }
    }

    public bool HaveMoreDocumentsInBucketAfter(int bucket, long after, out string merged)
    {
        merged = null;
        using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        using (context.OpenReadTransaction())
        {
            foreach (var _ in GetDocumentsByBucketFrom(context, bucket, after + 1))
            {
                return true;
            }

            merged = GetMergedChangeVectorInBucket(context, bucket);
            return false;
        }
    }

    public IEnumerable<ReplicationBatchItem> GetTombstonesByBucketFrom(DocumentsOperationContext context, int bucket, long etag) => 
        RetrieveTombstonesByBucketFrom(context, bucket, etag).Select(tombstone => TombstoneReplicationItem.From(context, tombstone));

    public IEnumerable<Tombstone> RetrieveTombstonesByBucketFrom(DocumentsOperationContext context, int bucket, long etag)
    {
        var table = new Table(TombstonesSchema, context.Transaction.InnerTransaction);

        foreach (var result in GetItemsByBucket(context.Allocator, table, TombstonesSchema.DynamicKeyIndexes[TombstonesBucketAndEtagSlice], bucket, etag))
        {
            yield return TableValueToTombstone(context, ref result.Result.Reader);
        }
    }

    public static IEnumerable<Table.SeekResult> GetItemsByBucket(ByteStringContext allocator, Table table,
        TableSchema.DynamicKeyIndexDef dynamicIndex, int bucket, long etag, long skip = 0, long take = long.MaxValue)
    {
        if (take <= 0)
            yield break;

        using (GetBucketAndEtagByteString(allocator, bucket, etag, out var buffer))
        using (Slice.External(allocator, buffer, buffer.Length, out var keySlice))
        using (Slice.External(allocator, buffer, buffer.Length - sizeof(long), out var prefix))
        {
            foreach (var result in table.SeekByPrefix(dynamicIndex, prefix, keySlice, skip))
            {
                yield return result;
                
                take--;
                if (take <= 0)
                    break;
            }
        }
    }

    private const long MaxDocumentsToDeleteInBucket = 1024;

    public ShardedDocumentDatabase.DeleteBucketCommand.DeleteBucketResult DeleteBucket(DocumentsOperationContext context, int bucket, ChangeVector upTo)
    {
        long deleted = 0;

        MarkTombstonesAsArtificial(context, bucket, upTo);
        var result = ShardedDocumentDatabase.DeleteBucketCommand.DeleteBucketResult.Empty;
        
        var docs = GetDocumentsByBucketFrom(context, bucket, 0, take: MaxDocumentsToDeleteInBucket, fields: DocumentFields.ChangeVector | DocumentFields.Id).ToList();
        foreach (var document in docs)
        {
            var docCv = context.GetChangeVector(document.ChangeVector);
            if (ChangeVectorUtils.GetConflictStatus(docCv, upTo) != ConflictStatus.AlreadyMerged)
            {
                result = ShardedDocumentDatabase.DeleteBucketCommand.DeleteBucketResult.Skipped;
                break;
            }

            // check change vectors of all document extensions
            if (HasDocumentExtensionWithGreaterChangeVector(context, document.Id, upTo))
            {
                result = ShardedDocumentDatabase.DeleteBucketCommand.DeleteBucketResult.Skipped;
                continue;
            }
            
            Delete(context, document.Id, flags: DocumentFlags.Artificial | DocumentFlags.FromResharding);

            // delete revisions for document
            RevisionsStorage.ForceDeleteAllRevisionsFor(context, document.Id, DocumentFlags.FromResharding | DocumentFlags.Artificial);
            deleted++;

            if (context.CanContinueTransaction == false)
                return ShardedDocumentDatabase.DeleteBucketCommand.DeleteBucketResult.ReachedTransactionLimit;
        }

        if (deleted >= MaxDocumentsToDeleteInBucket)
            return ShardedDocumentDatabase.DeleteBucketCommand.DeleteBucketResult.FullBatch;

        return result;
    }

    private void MarkTombstonesAsArtificial(DocumentsOperationContext context, int bucket, ChangeVector upTo)
    {
        long lastProcessedEtag = 0;
        bool hasMore = true, collectionNamesUpdated = false;
        var collectionNames = new Dictionary<string, CollectionName>(_collectionsCache, StringComparer.OrdinalIgnoreCase);

        while (hasMore)
        {
            hasMore = false;
            foreach (var tombstone in RetrieveTombstonesByBucketFrom(context, bucket, lastProcessedEtag))
            {
                if (tombstone.Flags.Contain(DocumentFlags.Artificial) && tombstone.Flags.Contain(DocumentFlags.FromResharding))
                    continue;

                var tombstoneChangeVector = context.GetChangeVector(tombstone.ChangeVector);
                if (ChangeVectorUtils.GetConflictStatus(upTo, tombstoneChangeVector) != ConflictStatus.AlreadyMerged)
                    break;

                var collection = tombstone.Collection;
                if (collectionNames.TryGetValue(collection, out var collectionName) == false)
                {
                    collectionNames[collection] = collectionName = new CollectionName(collection);
                    collectionNamesUpdated = true;
                }

                var writeTable = context.Transaction.InnerTransaction.OpenTable(TombstonesSchema, collectionName.GetTableName(CollectionTableType.Tombstones));

                var newEtag = GenerateNextEtag();
                var cv = ChangeVector.MergeWithDatabaseChangeVector(context, tombstoneChangeVector);
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
                DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Aviv, DevelopmentHelper.Severity.Normal, "optimize this, can be expensive to reopen each time");

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

    public override void ValidateId(DocumentsOperationContext context, Slice lowerId, DocumentChangeTypes type, DocumentFlags documentFlags = DocumentFlags.None)
    {
        DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Shiran, DevelopmentHelper.Severity.Normal, "need to make sure we check that for counters/TS/etc...");
        var config = _documentDatabase.ShardingConfiguration;
        var bucket = ShardHelper.GetBucketFor(config, lowerId);
        var shard = ShardHelper.GetShardNumberFor(config, bucket);
        if (shard != _documentDatabase.ShardNumber)
        {
            if (_documentDatabase.ForTestingPurposes != null && _documentDatabase.ForTestingPurposes.EnableWritesToTheWrongShard)
                return;

            if (documentFlags.Contain(DocumentFlags.FromReplication))
            {
                // RavenDB-21104
                // we allow writing the document to the wrong shard to avoid inconsistent data within the shard group
                // and handle the leftovers at the end of the transaction 
                context.Transaction.ExecuteDocumentsMigrationAfterCommit();
                return;
            }

            if (type == DocumentChangeTypes.Delete &&
                documentFlags.Contain(DocumentFlags.Artificial | DocumentFlags.FromResharding))
            {
                // RavenDB-22200
                // we allow deletion of documents from the wrong shard in case there are leftover
                // in the source shard due to a previous error during the bucket migration process
                return;
            }

            throw new ShardMismatchException($"Document '{lowerId}' belongs to bucket '{bucket}' on shard #{shard}, but {type} operation was performed on shard #{_documentDatabase.ShardNumber}.");
        }
    }
}
