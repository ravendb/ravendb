using System;

namespace Raven.Client.Data.Indexes
{
    public class IndexingPerformanceStats
    {
        public IndexingPerformanceStats(TimeSpan duration)
        {
            DurationInMilliseconds = Math.Round(duration.TotalMilliseconds, 2);
        }

        public int InputCount { get; set; }

        public int FailedCount { get; set; }

        public int OutputCount { get; set; }

        public int SuccessCount { get; set; }

        public DateTime Started { get; set; }

        public DateTime Completed { get; set; }

        public double DurationInMilliseconds { get; }

        public IndexingPerformanceOperation Details { get; set; }
    }
}
