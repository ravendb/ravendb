using System.Collections.Generic;
using Raven.Server.Documents.Sharding;
using Raven.Server.ServerWide.Context;
using Sparrow.Server;
using Voron;
using Voron.Data.Tables;
using Voron.Impl;
using static Raven.Server.Documents.Schemas.Revisions;

namespace Raven.Server.Documents.Revisions
{
    public partial class RevisionsStorage
    {
        public IEnumerable<Document> GetRevisionsByBucketFrom(DocumentsOperationContext context, int bucket, long etag)
        {
            var table = context.RevisionsTable(this);

            foreach (var result in ShardedDocumentsStorage.GetItemsByBucket(context.Allocator, table, RevisionsSchema.DynamicKeyIndexes[RevisionsBucketAndEtagSlice], bucket, etag))
            {
                yield return TableValueToRevision(context, result.Result);
            }
        }

        [StorageIndexEntryKeyGenerator]
        internal static ByteStringContext.Scope GenerateBucketAndEtagIndexKeyForRevisions(Transaction tx, in TableValueReader tvr, out Slice slice)
        {
            return ShardedDocumentsStorage.GenerateBucketAndEtagIndexKey(tx, idIndex: (int)RevisionsTable.LowerId, etagIndex: (int)RevisionsTable.Etag, tvr, out slice);
        }

        internal static void UpdateBucketStatsForRevisions(Transaction tx, Slice key, in TableValueReader oldValue, in TableValueReader newValue)
        {
            var changeVectorIndex = (int)RevisionsTable.FullChangeVector;
            if (newValue is { Size: > 0, Count: 12 })
                // this is a legacy revision record, which doesn't have the full change vector, so we need to use the revision PK as the change vector index
                changeVectorIndex = (int)RevisionsTable.RevisionPk;

            ShardedDocumentsStorage.UpdateBucketStatsInternal(tx, key, newValue, changeVectorIndex, sizeChange: newValue.Size - oldValue.Size);
        }

    }
}
