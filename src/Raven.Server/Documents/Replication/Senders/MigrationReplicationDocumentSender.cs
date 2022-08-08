using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using Raven.Server.Documents.Replication.Outgoing;
using Raven.Server.Documents.Replication.ReplicationItems;
using Raven.Server.Documents.Replication.Stats;
using Raven.Server.Documents.Sharding;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Logging;

namespace Raven.Server.Documents.Replication.Senders
{
    public class MigrationReplicationDocumentSender : ReplicationDocumentSenderBase
    {
        public const string MigrationTag = "MOVE";
        public readonly BucketMigrationReplication Destination;
        public readonly ShardedDocumentDatabase Database;
        public MigrationReplicationDocumentSender(Stream stream, OutgoingMigrationReplicationHandler parent, Logger log) : base(stream, parent, log)
        {
            Destination = parent.BucketMigrationNode;
            Database = (ShardedDocumentDatabase)parent._parent.Database;
        }

        protected override IEnumerable<ReplicationBatchItem> GetReplicationItems(DocumentsOperationContext ctx, long etag, ReplicationStats stats, bool caseInsensitiveCounters)
        {
            var database = ctx.DocumentDatabase;
            var bucket = Destination.Bucket;
            var migrationId = Database.ShardedDatabaseId;

            var docs = database.DocumentsStorage.GetDocumentsByBucketFrom(ctx, bucket, etag + 1).Select(DocumentReplicationItem.From);
            var tombs = database.DocumentsStorage.GetTombstonesByBucketFrom(ctx, bucket, etag + 1);
            var conflicts = database.DocumentsStorage.ConflictsStorage.GetConflictsByBucketFrom(ctx, bucket, etag + 1).Select(DocumentReplicationItem.From);
            var revisionsStorage = database.DocumentsStorage.RevisionsStorage;
            var revisions = revisionsStorage.GetRevisionsByBucketFrom(ctx, bucket, etag + 1).Select(DocumentReplicationItem.From);
            var attachments = database.DocumentsStorage.AttachmentsStorage.GetAttachmentsByBucketFrom(ctx, bucket, etag + 1);
            var counters = database.DocumentsStorage.CountersStorage.GetCountersByBucketFrom(ctx, bucket, etag + 1);
            var timeSeries = database.DocumentsStorage.TimeSeriesStorage.GetSegmentsByBucketFrom(ctx, bucket, etag + 1);
            var deletedTimeSeriesRanges = database.DocumentsStorage.TimeSeriesStorage.GetDeletedRangesByBucketFrom(ctx, bucket, etag + 1);

            using (var docsIt = docs.GetEnumerator())
            using (var tombsIt = tombs.GetEnumerator())
            using (var conflictsIt = conflicts.GetEnumerator())
            using (var versionsIt = revisions.GetEnumerator())
            using (var attachmentsIt = attachments.GetEnumerator())
            using (var countersIt = counters.GetEnumerator())
            using (var timeSeriesIt = timeSeries.GetEnumerator())
            using (var deletedTimeSeriesRangesIt = deletedTimeSeriesRanges.GetEnumerator())
            using (var mergedInEnumerator = new MergedReplicationBatchEnumerator(stats.DocumentRead, stats.AttachmentRead, stats.TombstoneRead, stats.CounterRead, stats.TimeSeriesRead))
            {
                mergedInEnumerator.AddEnumerator(ReplicationBatchItem.ReplicationItemType.Document, docsIt);
                mergedInEnumerator.AddEnumerator(ReplicationBatchItem.ReplicationItemType.DocumentTombstone, tombsIt);
                mergedInEnumerator.AddEnumerator(ReplicationBatchItem.ReplicationItemType.Document, conflictsIt);
                mergedInEnumerator.AddEnumerator(ReplicationBatchItem.ReplicationItemType.Document, versionsIt);
                mergedInEnumerator.AddEnumerator(ReplicationBatchItem.ReplicationItemType.Attachment, attachmentsIt);
                mergedInEnumerator.AddEnumerator(ReplicationBatchItem.ReplicationItemType.CounterGroup, countersIt);
                mergedInEnumerator.AddEnumerator(ReplicationBatchItem.ReplicationItemType.TimeSeriesSegment, timeSeriesIt);
                mergedInEnumerator.AddEnumerator(ReplicationBatchItem.ReplicationItemType.DeletedTimeSeriesRange, deletedTimeSeriesRangesIt);

                while (mergedInEnumerator.MoveNext())
                {

                    var item = mergedInEnumerator.Current;
                    var result = ChangeVectorUtils.TryUpdateChangeVector(MigrationTag, migrationId, Destination.MigrationIndex, item.ChangeVector);
                    Debug.Assert(result.IsValid,$"Failed to update the change vector {item.ChangeVector} with '{MigrationTag}:{Destination.MigrationIndex}-{migrationId}'");

                    item.ChangeVector = result.ChangeVector;
                    yield return item;
                }
            }
        }

        protected override bool ShouldSkip(ReplicationBatchItem item, OutgoingReplicationStatsScope stats, SkippedReplicationItemsInfo skippedReplicationItemsInfo)
        {
            switch (item)
            {
                case DocumentReplicationItem doc:
                    if (doc.Flags.Contain(DocumentFlags.Artificial))
                    {
                        stats.RecordArtificialDocumentSkip();
                        skippedReplicationItemsInfo.Update(item, isArtificial: true);
                        return true;
                    }

                    break;
            }

            return false;
        }
    }
}
