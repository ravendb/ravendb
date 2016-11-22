using System;

namespace Raven.NewClient.Data.Indexes
{
    public class IndexingPerformanceStats : IndexingPerformanceBasicStats
    {
        public IndexingPerformanceStats(TimeSpan duration) 
            : base(duration)
        {
        }

        public DateTime? Completed { get; set; }

        public IndexingPerformanceOperation Details { get; set; }
    }

    public class IndexingPerformanceBasicStats
    {
        public IndexingPerformanceBasicStats(TimeSpan duration)
        {
            DurationInMilliseconds = Math.Round(duration.TotalMilliseconds, 2);
        }

        public int InputCount { get; set; }

        public int FailedCount { get; set; }

        public int OutputCount { get; set; }

        public int SuccessCount { get; set; }

        public DateTime Started { get; set; }

        public double DurationInMilliseconds { get; }
    }
}
