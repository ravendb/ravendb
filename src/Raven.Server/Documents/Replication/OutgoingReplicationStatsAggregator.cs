using System.Diagnostics;
using Raven.Server.Utils.Stats;

namespace Raven.Server.Documents.Replication
{
    public class OutgoingReplicationStatsAggregator : StatsAggregator<OutgoingReplicationRunStats, OutgoingReplicationStatsScope>
    {
        public OutgoingReplicationStatsAggregator(int id, StatsAggregator<OutgoingReplicationRunStats, OutgoingReplicationStatsScope> lastStats)
            : base(id, lastStats)
        {
        }

        public override OutgoingReplicationStatsScope CreateScope()
        {
            Debug.Assert(Scope == null);

            return Scope = new OutgoingReplicationStatsScope(Stats);
        }

        public ReplicationPerformanceStats ToReplicationPerformanceLiveStatsWithDetails()
        {
            throw new System.NotImplementedException();
        }

        public ReplicationPerformanceStats ToReplicationPerformanceStats()
        {
            throw new System.NotImplementedException();
        }
    }

    public class OutgoingReplicationStatsScope : StatsScope<OutgoingReplicationRunStats, OutgoingReplicationStatsScope>
    {
        public OutgoingReplicationStatsScope(OutgoingReplicationRunStats stats, bool start = true)
            : base(stats, start)
        {
        }

        protected override OutgoingReplicationStatsScope OpenNewScope(OutgoingReplicationRunStats stats, bool start)
        {
            return new OutgoingReplicationStatsScope(stats, start);
        }

        public void RecordInputAttempt()
        {
            throw new System.NotImplementedException();
        }

        public void RecordArtificialDocumentSkip()
        {
            throw new System.NotImplementedException();
        }

        public void RecordSystemDocumentSkip()
        {
            throw new System.NotImplementedException();
        }

        public void RecordDocumentChangeVectorSkip()
        {
            throw new System.NotImplementedException();
        }

        public void RecordAttachmentOutput(long size)
        {
            throw new System.NotImplementedException();
        }

        public void RecordTombstoneOutput(long size)
        {
            throw new System.NotImplementedException();
        }

        public void RecordDocumentOutput(long size)
        {
            throw new System.NotImplementedException();
        }
    }

    public class OutgoingReplicationRunStats
    {
    }
}