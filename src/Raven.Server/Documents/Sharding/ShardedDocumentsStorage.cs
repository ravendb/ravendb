using System;
using System.Collections.Generic;
using Raven.Server.Documents.Replication.ReplicationItems;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Binary;
using Sparrow.Json;
using Sparrow.Server;
using Sparrow.Utils;
using Voron;
using Voron.Data.Tables;
using static Raven.Server.Documents.Schemas.Documents;
using static Raven.Server.Documents.Schemas.Tombstones;

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

    public long DeleteBucket(DocumentsOperationContext context, int bucket, ChangeVector upTo)
    {
        var deleted = 0L;
        foreach (var collectionName in _collectionsCache.Values)
        {
            deleted += DeleteItemsByBucketForDocuments(context, collectionName, bucket, upTo);
            deleted += DeleteItemsByBucketForTombstones(context, collectionName, bucket, upTo);
        }
        
        return deleted;
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
        DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Karmel, DevelopmentHelper.Severity.Normal,
            "Extend this for all types not only docs");

        var table = new Table(DocsSchema, context.Transaction.InnerTransaction);
        var merged = context.GetChangeVector(string.Empty);
        foreach (var result in GetItemsByBucket(context.Allocator, table, DocsSchema.DynamicKeyIndexes[AllDocsBucketAndEtagSlice], bucket, 0))
        {
            var document = TableValueToDocument(context, ref result.Result.Reader, DocumentFields.ChangeVector);
            merged = merged.MergeWith(document.ChangeVector, context);
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

    public const long MaxDocumentsToDeleteInBucket = 1024;

    public static long DeleteItemsByBucket(DocumentsOperationContext context, Table table,
        TableSchema.DynamicKeyIndexDef dynamicIndex, int changeVectorPosition, int bucket, ChangeVector upTo)
    {
        using (GetBucketByteString(context.Allocator, bucket, out var buffer))
        using (Slice.External(context.Allocator, buffer, buffer.Length, out var prefix))
        {
            return table.DeleteForwardFrom(dynamicIndex, prefix, startsWith: true, MaxDocumentsToDeleteInBucket, shouldAbort: holder =>
            {
                var changeVector = TableValueToChangeVector(context, changeVectorPosition, ref holder.Reader);
                var status = ChangeVectorUtils.GetConflictStatus(changeVector, upTo);
                return status != ConflictStatus.AlreadyMerged;
            });
        }
    }

    public static long DeleteItemsByBucketForDocuments(DocumentsOperationContext context, CollectionName collection, int bucket, ChangeVector upTo)
    {
        var table = context.Transaction.InnerTransaction.OpenTable(DocsSchema, collection.GetTableName(CollectionTableType.Documents));
        return DeleteItemsByBucket(context, table, DocsSchema.DynamicKeyIndexes[CollectionDocsBucketAndEtagSlice], (int)DocumentsTable.ChangeVector, bucket, upTo);
    }

    public static long DeleteItemsByBucketForTombstones(DocumentsOperationContext context, CollectionName collection, int bucket, ChangeVector upTo)
    {
        var table = context.Transaction.InnerTransaction.OpenTable(TombstonesSchema, collection.GetTableName(CollectionTableType.Tombstones));
        return DeleteItemsByBucket(context, table, TombstonesSchema.DynamicKeyIndexes[CollectionTombstonesBucketAndEtagSlice], (int)TombstoneTable.ChangeVector, bucket, upTo);
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
