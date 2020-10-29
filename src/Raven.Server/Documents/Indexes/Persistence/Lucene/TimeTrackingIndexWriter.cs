using Lucene.Net.Analysis;
using Lucene.Net.Index;
using Lucene.Net.Store;
using Raven.Client.Documents.Indexes;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene
{
    public class TimeTrackingIndexWriter : IndexWriter
    {
        private TimeTrackingSerialMergeScheduler _mergeScheduler;

        private IndexingStatsScope _commitStats;

        public TimeTrackingIndexWriter(Directory d, Analyzer a, IndexDeletionPolicy deletionPolicy, MaxFieldLength mfl, IState state) : base(d, a, deletionPolicy, mfl, state)
        {
        }

        public void InitializeMergeScheduler(TimeTrackingSerialMergeScheduler scheduler, IState state)
        {
            _mergeScheduler = scheduler;
            SetMergeScheduler(_mergeScheduler, state);
        }

        public void SetCommitStats(IndexingStatsScope commitStats)
        {
            _commitStats = commitStats;
            _mergeScheduler.SetCommitStats(commitStats);
        }

        protected override bool ApplyDeletes(IState state)
        {
            using (_commitStats?.For(IndexingOperation.Lucene.ApplyDeletes))
            {
                return base.ApplyDeletes(state);
            }
        }
    }
}
