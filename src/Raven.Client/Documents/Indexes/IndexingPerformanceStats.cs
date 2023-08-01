using System;
using Raven.Client.Util;

namespace Raven.Client.Documents.Indexes
{
    public sealed class IndexingPerformanceStats : IndexingPerformanceBasicStats
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
            AllocatedManagedBytes = new Size();
            AllocatedUnmanagedBytes = new Size();
            DurationInMs = Math.Round(duration.TotalMilliseconds, 2);
        }

        public long InputCount { get; set; }

        public long FailedCount { get; set; }

        public long OutputCount { get; set; }

        public long SuccessCount { get; set; }

        public DateTime Started { get; set; }

        public double DurationInMs { get; }

        public Size AllocatedManagedBytes { get; set; }
        
        public Size AllocatedUnmanagedBytes { get; set; }
        
        public Size DocumentsSize { get; set; }
    }
}
