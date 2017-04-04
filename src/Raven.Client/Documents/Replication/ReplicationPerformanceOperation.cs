using System;

namespace Raven.Client.Documents.Replication
{
    public class ReplicationPerformanceOperation
    {
        public ReplicationPerformanceOperation()
        {
            // for deserialization
        }

        public ReplicationPerformanceOperation(TimeSpan duration)
        {
            DurationInMilliseconds = Math.Round(duration.TotalMilliseconds, 2);
            Operations = new ReplicationPerformanceOperation[0];
        }

        public string Name { get; set; }

        public double DurationInMilliseconds { get; }

        public ReplicationPerformanceOperation[] Operations { get; set; }
    }
}