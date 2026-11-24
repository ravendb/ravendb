using System;
using System.Text.Json.Serialization;

namespace Raven.Server.Documents.ETL.Stats
{
    public class EtlPerformanceOperation
    {
        public EtlPerformanceOperation(TimeSpan duration)
        {
            DurationInMs = Math.Round(duration.TotalMilliseconds, 2);
            Operations = [];
        }

        [JsonConstructor]
        private EtlPerformanceOperation(double durationInMs)
        {
            DurationInMs = durationInMs;
            Operations = [];
        }

        public string Name { get; set; }

        public double DurationInMs { get; }

        public EtlPerformanceOperation[] Operations { get; set; }
    }
}
