using System.Collections.Generic;
using Raven.Server.Documents.Replication.ReplicationItems;
using Raven.Server.Documents.Sharding;
using Raven.Server.ServerWide.Context;
using Sparrow.Server;
using Voron;
using Voron.Data.Tables;
using Voron.Impl;
using static Raven.Server.Documents.Schemas.Counters;
using static Raven.Server.Documents.Schemas.CounterTombstones;

namespace Raven.Server.Documents
{
    public partial class CountersStorage
    {
        public IEnumerable<ReplicationBatchItem> GetCountersByBucketFrom(DocumentsOperationContext context, int bucket, long etag)
        {
            var table = new Table(CountersSchema, context.Transaction.InnerTransaction);

            foreach (var result in ShardedDocumentsStorage.GetItemsByBucket(context.Allocator, table, CountersSchema.DynamicKeyIndexes[CountersBucketAndEtagSlice], bucket, etag))
            {
                yield return CreateReplicationBatchItem(context, ref result.Result.Reader);
            }
        }

        public IEnumerable<CounterTombstoneDetail> GetCounterTombstonesByBucketFrom(DocumentsOperationContext context, int bucket, long etag)
        {
            var table = new Table(CounterTombstonesSchema, context.Transaction.InnerTransaction);

            foreach (var result in ShardedDocumentsStorage.GetItemsByBucket(context.Allocator, table, CounterTombstonesSchema.DynamicKeyIndexes[CounterTombstonesBucketAndEtagSlice], bucket, etag))
            {
                yield return TableValueToCounterTombstoneDetail(context, ref result.Result.Reader);
            }
        }

        [StorageIndexEntryKeyGenerator]
        internal static ByteStringContext.Scope GenerateBucketAndEtagIndexKeyForCounters(Transaction tx, ref TableValueReader tvr, out Slice slice)
        {
            return ShardedDocumentsStorage.ExtractIdFromKeyAndGenerateBucketAndEtagIndexKey(tx, (int)CountersTable.CounterKey, (int)CountersTable.Etag, ref tvr, out slice);
        }

        [StorageIndexEntryKeyGenerator]
        internal static ByteStringContext.Scope GenerateBucketAndEtagIndexKeyForCounterTombstones(Transaction tx, ref TableValueReader tvr, out Slice slice)
        {
            return ShardedDocumentsStorage.ExtractIdFromKeyAndGenerateBucketAndEtagIndexKey(tx, (int)CounterTombstonesTable.CounterTombstoneKey, (int)CounterTombstonesTable.Etag, ref tvr, out slice);
        }

        internal static void UpdateBucketStatsForCounters(Transaction tx, Slice key, ref TableValueReader oldValue, ref TableValueReader newValue)
        {
            ShardedDocumentsStorage.UpdateBucketStatsInternal(tx, key, ref newValue, changeVectorIndex: (int)CountersTable.ChangeVector, sizeChange: newValue.Size - oldValue.Size);
        }

        internal static void UpdateBucketStatsForCounterTombstones(Transaction tx, Slice key, ref TableValueReader oldValue, ref TableValueReader newValue)
        {
            // counter tombstones are not replicated, no need to update the merged-cv of the bucket 
            ShardedDocumentsStorage.UpdateBucketStatsInternal(tx, key, sizeChange: newValue.Size - oldValue.Size);
        }
    }
}
