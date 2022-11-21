using System.Collections.Generic;
using Raven.Server.Documents.Sharding;
using Raven.Server.ServerWide.Context;
using Sparrow.Server;
using Voron;
using Voron.Data.Tables;
using static Raven.Server.Documents.Schemas.Revisions;

namespace Raven.Server.Documents.Revisions
{
    public partial class RevisionsStorage
    {
        public IEnumerable<Document> GetRevisionsByBucketFrom(DocumentsOperationContext context, int bucket, long etag)
        {
            var table = new Table(RevisionsSchema, context.Transaction.InnerTransaction);

            foreach (var result in ShardedDocumentsStorage.GetItemsByBucket(context.Allocator, table, RevisionsSchema.DynamicKeyIndexes[RevisionsBucketAndEtagSlice], bucket, etag))
            {
                yield return TableValueToRevision(context, ref result.Result.Reader);
            }
        }

        [StorageIndexEntryKeyGenerator]
        internal static ByteStringContext.Scope GenerateBucketAndEtagIndexKeyForRevisions(ByteStringContext context, ref TableValueReader tvr, out Slice slice)
        {
            return ShardedDocumentsStorage.GenerateBucketAndEtagIndexKey(context, idIndex: (int)RevisionsTable.LowerId, etagIndex: (int)RevisionsTable.Etag, ref tvr, out slice);
        }
    }
}
