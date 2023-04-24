using System;
using Raven.Server.Documents.Replication.ReplicationItems;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Raven.Server.Documents.Sharding;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Binary;
using Sparrow.Server;
using Voron;
using Voron.Data.Tables;
using Voron.Impl;
using static Raven.Server.Documents.Schemas.Attachments;

namespace Raven.Server.Documents
{
    public unsafe partial class AttachmentsStorage
    {
        public IEnumerable<AttachmentReplicationItem> GetAttachmentsByBucketFrom(DocumentsOperationContext context, int bucket, long etag)
        {
            var table = context.Transaction.InnerTransaction.OpenTable(AttachmentsSchema, AttachmentsMetadataSlice);

            foreach (var result in ShardedDocumentsStorage.GetItemsByBucket(context.Allocator, table, AttachmentsSchema.DynamicKeyIndexes[AttachmentsBucketAndEtagSlice], bucket, etag))
            {
                var attachment = TableValueToAttachment(context, ref result.Result.Reader);

                var stream = GetAttachmentStream(context, attachment.Base64Hash);
                if (stream == null)
                    ThrowMissingAttachment(attachment.Name);

                attachment.Stream = stream;

                yield return AttachmentReplicationItem.From(context, attachment);
            }
        }

        [StorageIndexEntryKeyGenerator]
        internal static ByteStringContext.Scope GenerateBucketAndEtagIndexKeyForAttachments(Transaction tx, ref TableValueReader tvr, out Slice slice)
        {
            return ShardedDocumentsStorage.ExtractIdFromKeyAndGenerateBucketAndEtagIndexKey(tx, (int)AttachmentsTable.LowerDocumentIdAndLowerNameAndTypeAndHashAndContentType,
                (int)AttachmentsTable.Etag, ref tvr, out slice);
        }

        [StorageIndexEntryKeyGenerator]
        internal static ByteStringContext.Scope GenerateBucketAndHashForAttachments(Transaction tx, ref TableValueReader tvr, out Slice slice)
        {
            return GenerateBucketAndHash(tx, (int)AttachmentsTable.LowerDocumentIdAndLowerNameAndTypeAndHashAndContentType, (int)AttachmentsTable.Hash, ref tvr, out slice);
        }

        private static ByteStringContext.Scope GenerateBucketAndHash(Transaction tx, int keyIndex, int hashIndex, ref TableValueReader tvr, out Slice slice)
        {
            var docsCtx = tx.Owner as DocumentsOperationContext;
            var database = ShardedDocumentDatabase.CastToShardedDocumentDatabase(docsCtx!.DocumentDatabase);

            var bucket = GetBucketFromAttachmentKey(keyIndex, tvr, database);
            bucket = Bits.SwapBytes(bucket);

            var hashPtr = tvr.Read(hashIndex, out var hashSize);
            var scope = tx.Allocator.Allocate(sizeof(int) + hashSize, out var buffer);

            var span = new Span<byte>(buffer.Ptr, buffer.Length);
            MemoryMarshal.AsBytes(new Span<int>(ref bucket)).CopyTo(span);
            new ReadOnlySpan<byte>(hashPtr, hashSize).CopyTo(span[sizeof(int)..]);

            slice = new Slice(buffer);
            return scope;
        }

        private static int GetBucketFromAttachmentKey(int keyIndex, TableValueReader tvr, ShardedDocumentDatabase database)
        {
            var keyPtr = tvr.Read(keyIndex, out var keySize);
            int sizeOfDocId = GetSizeOfDocId(new ReadOnlySpan<byte>(keyPtr, keySize));
            var keySpan = new ReadOnlySpan<byte>(keyPtr, sizeOfDocId);
            return ShardHelper.GetBucketFor(database.ShardingConfiguration, keySpan);
        }

        public (long TotalSize, int UniqueAttachmets) GetStreamInfoForBucket(Transaction tx, int bucket)
        {
            var table = tx.OpenTable(AttachmentsSchema, AttachmentsMetadataSlice);
            var rawBucket = stackalloc byte[sizeof(int)];
            *(int*)rawBucket = Bits.SwapBytes(bucket);

            var total = 0L;
            var count = 0;
            var tree = tx.CreateTree(AttachmentsSlice);

            using (Slice.External(tx.Allocator, rawBucket, sizeof(int), ByteStringType.Immutable, out var bucketSlice))
            {
                var startSlice = bucketSlice;
                foreach (var result in table.SeekForwardFromPrefix(AttachmentsSchema.DynamicKeyIndexes[AttachmentsBucketAndHashSlice], startSlice, bucketSlice, skip: 0))
                {
                    if (SliceStructComparer.Instance.Equals(startSlice, result.Key))
                        continue;

                    count++;
                    var key = result.Key.Content;
                    using (Slice.External(tx.Allocator, key.Ptr + sizeof(int), key.Length - sizeof(int), out var hash))
                    {
                        var info = tree.GetStreamInfo(hash, writable: false);
                        total += info->TotalSize;
                    }

                    startSlice = result.Key.Clone(tx.Allocator);
                }
            }

            return (total, count);
        }

        internal static void UpdateBucketStatsForAttachments(Transaction tx, Slice key, ref TableValueReader oldValue, ref TableValueReader newValue)
        {
            if (tx.Owner is not DocumentsOperationContext { DocumentDatabase: ShardedDocumentDatabase documentDatabase })
            {
                Debug.Assert(false,$"tx.Owner is not DocumentsOperationContext");
                return;
            }

            var streamSize = 0L;
            var tree = tx.CreateTree(AttachmentsSlice);

            var tvr = oldValue.Pointer != null ? oldValue : newValue;
            var delete = oldValue.Pointer != null && newValue.Pointer == null;

            var oldHashPtr = tvr.Read((int)AttachmentsTable.Hash, out var oldHashSize);
            var size = sizeof(int) + oldHashSize;
            var buffer = stackalloc byte[size];

            var old = new Span<byte>(oldHashPtr, oldHashSize);
            var bufferAsSpan = new Span<byte>(buffer, size);
            key.AsReadOnlySpan().Slice(0, sizeof(int)).CopyTo(bufferAsSpan);
            old.CopyTo(bufferAsSpan.Slice(sizeof(int)));

            var schema = documentDatabase.ShardedDocumentsStorage.AttachmentsStorage.AttachmentsSchema;
            var table = tx.OpenTable(schema, AttachmentsMetadataSlice);
            using (Slice.External(tx.Allocator, buffer, sizeof(int) + oldHashSize, ByteStringType.Immutable, out var slice))
            using (Slice.External(tx.Allocator, oldHashPtr, oldHashSize, ByteStringType.Immutable, out var hashSlice))
            {
                var refCount = table.GetCountOfMatchesFor(schema.DynamicKeyIndexes[AttachmentsBucketAndHashSlice], slice);
                switch (refCount)
                {
                    case 1:
                        if (delete)
                            goto default;
                        
                        // unique stream for this bucket was add
                        var info = tree.GetStreamInfo(hashSlice, writable: false);
                        Debug.Assert(info != null, $"Try to add stream {hashSlice}, but it is missing");
                        streamSize = info->TotalSize;
                        break;
                    case 0:
                        // unique stream for this bucket was remove
                        info = tree.GetStreamInfo(hashSlice, writable: false);
                        Debug.Assert(info != null, $"Try to remove stream {hashSlice}, but it is missing");
                        streamSize = -info->TotalSize;
                        break;
                    default:
                        streamSize = 0; // reference to this stream was added/removed
                        break;
                }
            }

            ShardedDocumentsStorage.UpdateBucketStatsInternal(tx, key, ref newValue, changeVectorIndex: (int)AttachmentsTable.ChangeVector, sizeChange: newValue.Size - oldValue.Size + streamSize);
        }
    }
}
