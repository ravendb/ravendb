using System;
using System.Diagnostics;
using Lucene.Net.Index;
using Lucene.Net.Store;
using Raven.Client.Documents.Indexes;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene
{
    public class TimeTrackingSerialMergeScheduler : MergeScheduler
    {
        private readonly Index _index;
        private readonly TimeSpan _maxMergeTime;
        private IndexingStatsScope _commitStats;

        public TimeTrackingSerialMergeScheduler(Index index)
        {
            _index = index;
            _maxMergeTime = index.Configuration.MaxTimeForMergesToKeepRunning.AsTimeSpan;
        }
        
        public void SetCommitStats(IndexingStatsScope commitStats)
        {
            _commitStats = commitStats;
        }
        
        public override void Merge(IndexWriter writer, IState state)
        {
            using (var mergeStats = _commitStats?.For(IndexingOperation.Lucene.Merge))
            {
                var sp = Stopwatch.StartNew();
                lock (this)
                {
                    var totalMergesCount = writer.PendingMergesCount;
                        
                    mergeStats?.RecordPendingMergesCount(totalMergesCount);
                    var executedMerges = 0;

                    while (true)
                    {
                        if (sp.Elapsed > _maxMergeTime)
                        {
                            if (writer.PendingMergesCount > 0)
                                _index.ScheduleIndexingRun(); // we stop before we are done merging, force a new batch
                            break;
                        }

                        MergePolicy.OneMerge merge = writer.GetNextMerge();
                        if (merge == null)
                            break;

                        executedMerges++;
                        mergeStats?.RecordMergeStats(merge.Stats);
                        writer.Merge(merge, state);

                        var diff = writer.PendingMergesCount - totalMergesCount + executedMerges;
                        if (diff > 0)
                        {
                            // more merges can be created after a successful merge
                            mergeStats?.RecordPendingMergesCount(diff);
                            totalMergesCount += diff;
                        }
                    }
                }
            }
        }

        protected override void Dispose(bool disposing)
        {
            
        }
    }
}
