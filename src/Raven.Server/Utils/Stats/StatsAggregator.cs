using System;
using Raven.Client.Util;

namespace Raven.Server.Utils.Stats
{
    public interface IStatsAggregator
    {
        DateTime StartTime { get; }

        IStatsScope StatsScope { get; }
    }

    public abstract class StatsAggregator<TStats, TStatsScope> : IStatsAggregator
        where TStats : class, new() 
        where TStatsScope : StatsScope<TStats, TStatsScope>
    {
        public readonly int Id;

        private bool _completed;

        protected readonly TStats Stats;

        protected TStatsScope Scope;

        public IStatsScope StatsScope => Scope;

        protected StatsAggregator(int id, IStatsAggregator lastStats)
        {
            Id = id;

            var now = SystemTime.UtcNow;
            var currentScope = lastStats?.StatsScope;
            if (currentScope == null)
            {
                StartTime = now;
            }
            else
            {
                var lastCompleted = lastStats.StartTime.Add(currentScope.Duration);

                // due to different precision of DateTimes and Stopwatches we might have current date 
                // smaller than completed one of the latest batch
                // let us adjust current start to avoid overlapping on the performance graph

                StartTime = lastCompleted > now ? lastCompleted : now;
            }

            Stats = new TStats();
        }

        public void Complete()
        {
            _completed = true;
        }

        public bool Completed => _completed;

        public DateTime StartTime { get; }

        public TStats ToIndexingBatchStats()
        {
            return Stats;
        }

        public abstract TStatsScope CreateScope();
    }
}
