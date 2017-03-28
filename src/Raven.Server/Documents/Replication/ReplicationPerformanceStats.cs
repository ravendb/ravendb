using System;

namespace Raven.Server.Documents.Replication
{
    public class ReplicationPerformanceStats : ReplicationPerformanceBasicStats
    {
        public ReplicationPerformanceStats()
        {
            // for deserialization
        }

        public ReplicationPerformanceStats(TimeSpan duration)
            : base(duration)
        {
        }

        public int Id { get; set; }

        public DateTime? Completed { get; set; }

        public ReplicationPerformanceOperation Details { get; set; }
    }

    public class ReplicationPerformanceBasicStats
    {
        public ReplicationPerformanceBasicStats()
        {
            // for deserialization
        }

        public ReplicationPerformanceBasicStats(TimeSpan duration)
        {
            DurationInMilliseconds = Math.Round(duration.TotalMilliseconds, 2);
        }

        public DateTime Started { get; set; }

        public double DurationInMilliseconds { get; }
    }
}
