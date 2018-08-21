using System;
using System.Collections.Generic;

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

        public Dictionary<EtlItemType, long> LastTransformedEtag { get; set; }

        public Dictionary<EtlItemType, int> NumberOfExtractedItems { get; set; }

        public string BatchCompleteReason { get; set; }
    }
}
