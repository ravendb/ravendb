using System.Threading;
using Lucene.Net.Index;
using Lucene.Net.Store;
using Raven.Client.Documents.Indexes;
using Raven.Server.Utils;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene
{
    public class CustomSerialMergeScheduler : SerialMergeScheduler
    {
        private IndexingStatsScope _commitStats;

        public void SetCommitStats(IndexingStatsScope commitStats)
        {
            _commitStats = commitStats;
        }

        public override void Merge(IndexWriter writer, IState state)
        {
            using (_commitStats?.For(IndexingOperation.Lucene.Merge))
            {
                base.Merge(writer, state);
            }
        }
    }
}
