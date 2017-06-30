using System;

namespace Raven.Server.Documents.ETL.Stats
{
    public class EtlPerformanceStats
    {
        public EtlPerformanceStats(TimeSpan duration)
        {
            DurationInMs = Math.Round(duration.TotalMilliseconds, 2);
        }

        public int Id { get; set; }

        public DateTime Started { get; set; }

        public DateTime? Completed { get; set; }

        public double DurationInMs { get; }

        public EtlPerformanceOperation Details { get; set; }

        public long LastLoadedEtag { get; set; }

        public long LastTransformedEtag { get; set; }

        public int NumberOfExtractedItems { get; set; }

        public string BatchCompleteReason { get; set; }
    }
}