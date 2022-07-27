using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using JetBrains.Annotations;
using Raven.Server.Documents.Replication.ReplicationItems;
using Raven.Server.Documents.Replication.Stats;

namespace Raven.Server.Documents.Replication.Senders
{
    public class ReplicationBatchItemByEtagComparer : Comparer<ReplicationBatchItem>
    {
        public static ReplicationBatchItemByEtagComparer Instance = new ReplicationBatchItemByEtagComparer();

        public override int Compare(ReplicationBatchItem x, ReplicationBatchItem y)
        {
            var diff = y.Etag - x.Etag;
            
            if (diff < 0)
                return -1;
            if (diff > 0)
                return 1;
            return 0;
        }
    }

    public class MergedReplicationBatchEnumerator : MergedEnumerator<ReplicationBatchItem>
    {
        private readonly OutgoingReplicationStatsScope _documentRead;
        private readonly OutgoingReplicationStatsScope _attachmentRead;
        private readonly OutgoingReplicationStatsScope _tombstoneRead;
        private readonly OutgoingReplicationStatsScope _countersRead;
        private readonly OutgoingReplicationStatsScope _timeSeriesRead;

        public MergedReplicationBatchEnumerator(
            OutgoingReplicationStatsScope documentRead,
            OutgoingReplicationStatsScope attachmentRead,
            OutgoingReplicationStatsScope tombstoneRead,
            OutgoingReplicationStatsScope counterRead,
            OutgoingReplicationStatsScope timeSeriesRead
        ) : base(ReplicationBatchItemByEtagComparer.Instance)
        {
            _documentRead = documentRead;
            _attachmentRead = attachmentRead;
            _tombstoneRead = tombstoneRead;
            _countersRead = counterRead;
            _timeSeriesRead = timeSeriesRead;
        }

        public void AddEnumerator(ReplicationBatchItem.ReplicationItemType type, IEnumerator<ReplicationBatchItem> enumerator)
        {
            if (enumerator == null)
                return;

            if (enumerator.MoveNext())
            {
                using (GetStatsFor(type).Start())
                {
                    _workEnumerators.Add(enumerator);
                }
            }
        }

        public override void AddEnumerator(IEnumerator<ReplicationBatchItem> enumerator) => throw new NotSupportedException("Use AddEnumerator(ReplicationBatchItem.ReplicationItemType type, IEnumerator<ReplicationBatchItem> enumerator)");

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private OutgoingReplicationStatsScope GetStatsFor(ReplicationBatchItem.ReplicationItemType type)
        {
            switch (type)
            {
                case ReplicationBatchItem.ReplicationItemType.Document:
                    return _documentRead;

                case ReplicationBatchItem.ReplicationItemType.Attachment:
                    return _attachmentRead;

                case ReplicationBatchItem.ReplicationItemType.CounterGroup:
                    return _countersRead;

                case ReplicationBatchItem.ReplicationItemType.DocumentTombstone:
                case ReplicationBatchItem.ReplicationItemType.AttachmentTombstone:
                case ReplicationBatchItem.ReplicationItemType.RevisionTombstone:
                    return _tombstoneRead;

                case ReplicationBatchItem.ReplicationItemType.TimeSeriesSegment:
                case ReplicationBatchItem.ReplicationItemType.DeletedTimeSeriesRange:
                    return _timeSeriesRead;

                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        public override bool MoveNext()
        {
            if (_workEnumerators.Count == 0)
                return false;

            var current = _workEnumerators[0];
            for (var index = 1; index < _workEnumerators.Count; index++)
            {
                if (_workEnumerators[index].Current.Etag < current.Current.Etag)
                {
                    current = _workEnumerators[index];
                }
            }

            _currentItem = current.Current;
            using (GetStatsFor(_currentItem.Type).Start())
            {
                if (current.MoveNext() == false)
                {
                    _workEnumerators.Remove(current);
                }

                return true;
            }
        }
    }
}
