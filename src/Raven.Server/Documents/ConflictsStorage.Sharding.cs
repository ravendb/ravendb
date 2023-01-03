using System.Collections.Generic;
using Raven.Server.Documents.Sharding;
using Raven.Server.ServerWide.Context;
using Sparrow.Server;
using Voron;
using Voron.Data.Tables;
using Voron.Impl;
using static Raven.Server.Documents.Schemas.Conflicts;

namespace Raven.Server.Documents
{
    public partial class ConflictsStorage
    {
        public IEnumerable<DocumentConflict> GetConflictsByBucketFrom(DocumentsOperationContext context, int bucket, long etag)
        {
            var table = context.Transaction.InnerTransaction.OpenTable(ConflictsSchema, ConflictsSlice);

            foreach (var result in ShardedDocumentsStorage.GetItemsByBucket(context.Allocator, table, ConflictsSchema.DynamicKeyIndexes[ConflictsBucketAndEtagSlice], bucket, etag))
            {
                yield return TableValueToConflictDocument(context, ref result.Result.Reader);
            }
        }

        [StorageIndexEntryKeyGenerator]
        internal static ByteStringContext.Scope GenerateBucketAndEtagIndexKeyForConflicts(Transaction tx, ref TableValueReader tvr, out Slice slice)
        {
            return ShardedDocumentsStorage.GenerateBucketAndEtagIndexKey(tx, idIndex: (int)ConflictsTable.LowerId, etagIndex: (int)ConflictsTable.Etag, ref tvr, out slice);
        }
    }
}
