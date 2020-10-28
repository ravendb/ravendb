using Lucene.Net.Analysis;
using Lucene.Net.Index;
using Lucene.Net.Store;
using Raven.Client.Documents.Indexes;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene
{
    public class CustomIndexWriter : IndexWriter
    {
        private IndexingStatsScope _commitStats;

        public CustomIndexWriter(Directory d, Analyzer a, bool create, MaxFieldLength mfl, IState state) : base(d, a, create, mfl, state)
        {
        }

        public CustomIndexWriter(Directory d, Analyzer a, MaxFieldLength mfl, IState state) : base(d, a, mfl, state)
        {
        }

        public CustomIndexWriter(Directory d, Analyzer a, IndexDeletionPolicy deletionPolicy, MaxFieldLength mfl, IState state) : base(d, a, deletionPolicy, mfl, state)
        {
        }

        public CustomIndexWriter(Directory d, Analyzer a, bool create, IndexDeletionPolicy deletionPolicy, MaxFieldLength mfl, IState state) : base(d, a, create, deletionPolicy, mfl, state)
        {
        }

        public CustomIndexWriter(Directory d, Analyzer a, IndexDeletionPolicy deletionPolicy, MaxFieldLength mfl, IndexCommit commit, IState state) : base(d, a, deletionPolicy, mfl, commit, state)
        {
        }

        public void SetCommitStats(IndexingStatsScope commitStats)
        {
            _commitStats = commitStats;
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
