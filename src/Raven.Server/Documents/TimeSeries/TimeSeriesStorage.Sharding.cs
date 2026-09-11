using System.Collections.Generic;
using Raven.Server.Documents.Replication.ReplicationItems;
using Raven.Server.Documents.Sharding;
using Raven.Server.ServerWide.Context;
using Sparrow.Server;
using Voron;
using Voron.Data.Tables;
using Voron.Impl;
using static Raven.Server.Documents.Schemas.DeletedRanges;
using static Raven.Server.Documents.Schemas.TimeSeries;

namespace Raven.Server.Documents.TimeSeries
{
    public partial class TimeSeriesStorage
    {
        public IEnumerable<TimeSeriesReplicationItem> GetSegmentsByBucketFrom(DocumentsOperationContext context, int bucket, long etag, bool includeDocumentChangeVector = true)
        {
            var table = context.TimesSeriesTable(this);

            foreach (var result in ShardedDocumentsStorage.GetItemsByBucket(context.Allocator, table, Schemas.TimeSeries.TimeSeriesBucketAndEtagIndex, bucket, etag))
            {
                yield return CreateTimeSeriesSegmentItem(context, result.Result, includeDocumentChangeVector);
            }
        }

        public IEnumerable<TimeSeriesDeletedRangeItem> GetDeletedRangesByBucketFrom(DocumentsOperationContext context, int bucket, long etag)
        {
            var table = context.DeleteRangesTable(this);

            foreach (var result in ShardedDocumentsStorage.GetItemsByBucket(context.Allocator, table, Schemas.DeletedRanges.DeletedRangesBucketAndEtagIndex, bucket, etag))
            {
                yield return CreateDeletedRangeItem(context, result.Result);
            }
        }

        [StorageIndexEntryKeyGenerator]
        internal static ByteStringContext.Scope GenerateBucketAndEtagIndexKeyForTimeSeries(Transaction tx, in TableValueReader tvr, out Slice slice)
        {
            return ShardedDocumentsStorage.ExtractIdFromKeyAndGenerateBucketAndEtagIndexKey(tx, keyIndex: (int)TimeSeriesTable.TimeSeriesKey,
                etagIndex: (int)TimeSeriesTable.Etag, tvr, out slice);
        }

        [StorageIndexEntryKeyGenerator]
        internal static ByteStringContext.Scope GenerateBucketAndEtagIndexKeyForDeletedRanges(Transaction tx, in TableValueReader tvr, out Slice slice)
        {
            return ShardedDocumentsStorage.ExtractIdFromKeyAndGenerateBucketAndEtagIndexKey(tx, keyIndex: (int)DeletedRangeTable.RangeKey,
                etagIndex: (int)DeletedRangeTable.Etag, tvr, out slice);
        }

        internal static void UpdateBucketStatsForDeletedRanges(Transaction tx, Slice key, in TableValueReader oldValue, in TableValueReader newValue)
        {
            ShardedDocumentsStorage.UpdateBucketStatsInternal(tx, key, newValue, changeVectorIndex: (int)DeletedRangeTable.ChangeVector, sizeChange: newValue.Size - oldValue.Size);
        }

        internal static void UpdateBucketStatsForTimeSeries(Transaction tx, Slice key, in TableValueReader oldValue, in TableValueReader newValue)
        {
            ShardedDocumentsStorage.UpdateBucketStatsInternal(tx, key, newValue, changeVectorIndex: (int)TimeSeriesTable.ChangeVector, sizeChange: newValue.Size - oldValue.Size);
        }
    }
}
