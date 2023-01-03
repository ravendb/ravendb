using Raven.Server.Documents.Replication.ReplicationItems;
using System.Collections.Generic;
using Raven.Server.Documents.Sharding;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Binary;
using Sparrow.Server;
using Sparrow.Server.Utils;
using Voron;
using Voron.Data.Tables;
using static Raven.Server.Documents.Schemas.Attachments;

namespace Raven.Server.Documents
{
    public unsafe partial class AttachmentsStorage
    {
        public IEnumerable<ReplicationBatchItem> GetAttachmentsByBucketFrom(DocumentsOperationContext context, int bucket, long etag)
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
        internal static ByteStringContext.Scope GenerateBucketAndEtagIndexKeyForAttachments(ByteStringContext context, ref TableValueReader tvr, out Slice slice)
        {
            return ShardedDocumentsStorage.ExtractIdFromKeyAndGenerateBucketAndEtagIndexKey(context, (int)AttachmentsTable.LowerDocumentIdAndLowerNameAndTypeAndHashAndContentType,
                (int)AttachmentsTable.Etag, ref tvr, out slice);
        }

        private void UpdateBucketStatsOnPutOrDeleteStream(DocumentsOperationContext context, Slice attachmentKey, long sizeChange)
        {
            if (_documentDatabase is not ShardedDocumentDatabase)
                return;

            using (GetBucketFromAttachmentKey(context, attachmentKey, out var bucketSlice))
            {
                ShardedDocumentsStorage.UpdateBucketStats(context.Transaction.InnerTransaction, bucketSlice, oldSize: 0, newSize: sizeChange);
            }
        }

        private static ByteStringContext.Scope GetBucketFromAttachmentKey(DocumentsOperationContext context, Slice attachmentKey, out Slice bucketSlice)
        {
            int sizeOfDocId = 0;
            for (; sizeOfDocId < attachmentKey.Size; sizeOfDocId++)
            {
                if (attachmentKey[sizeOfDocId] == SpecialChars.RecordSeparator)
                    break;
            }

            var bucket = ShardHelper.GetBucketFor(attachmentKey.Content.Ptr, sizeOfDocId);
            var scope = context.Allocator.Allocate(sizeof(int), out var buffer);
            *(int*)buffer.Ptr = Bits.SwapBytes(bucket);

            bucketSlice = new Slice(buffer);

            return scope;
        }
    }
}
