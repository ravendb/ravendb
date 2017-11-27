using System;
using Raven.Client.Util;

namespace Raven.Client.Documents.Indexes
{
    public class IndexingPerformanceStats : IndexingPerformanceBasicStats
    {
        public IndexingPerformanceStats()
        {
            // for deserialization
        }

        public IndexingPerformanceStats(TimeSpan duration)
            : base(duration)
        {
        }

        public int Id { get; set; }

        public DateTime? Completed { get; set; }

        public IndexingPerformanceOperation Details { get; set; }
    }

    public class IndexingPerformanceBasicStats
    {
        public IndexingPerformanceBasicStats()
        {
            // for deserialization
        }

        public IndexingPerformanceBasicStats(TimeSpan duration)
        {
            AllocatedBytes = new Size();
            DurationInMs = Math.Round(duration.TotalMilliseconds, 2);
        }

        public int InputCount { get; set; }

        public int FailedCount { get; set; }

        public int OutputCount { get; set; }

        public int SuccessCount { get; set; }

        public DateTime Started { get; set; }

        public double DurationInMs { get; }

        public Size AllocatedBytes { get; set; }
        
        public Size DocumentsSize { get; set; }
    }
}
