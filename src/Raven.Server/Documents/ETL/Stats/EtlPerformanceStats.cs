using System;

namespace Raven.Server.Documents.ETL.Stats
{
    public class EtlPerformanceStats
    {
        public int Id { get; set; }

        public DateTime Started { get; set; }

        public DateTime? Completed { get; set; }

        public TimeSpan Duration { get; set; }

        public EtlPerformanceOperation Details { get; set; }
        public long LastLoadedEtag { get; set; }

        public long LastTransformedEtag { get; set; }

        public int NumberOfExtractedItems { get; set; }
    }
}