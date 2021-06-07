using System;

namespace Raven.Server.Documents.ETL.Stats
{
    public class EtlPerformanceOperation
    {
        public EtlPerformanceOperation(TimeSpan duration)
        {
            DurationInMs = Math.Round(duration.TotalMilliseconds, 2);
            Operations = new EtlPerformanceOperation[0];
        }

        public string Name { get; set; }

        public double DurationInMs { get; }

        public EtlPerformanceOperation[] Operations { get; set; }
    }
}
