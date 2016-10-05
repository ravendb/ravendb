using Sparrow.Json;
using System;

namespace Raven.Client.Data.Indexes
{
    public class IndexingPerformanceOperation
    {
        public IndexingPerformanceOperation(TimeSpan duration)
        {
            DurationInMilliseconds = Math.Round(duration.TotalMilliseconds, 2);
            Operations = new IndexingPerformanceOperation[0];
        }

        public string Name { get; set; }

        public double DurationInMilliseconds { get; }

        public ReduceRunDetails ReduceDetails { get; set; }

        public MapRunDetails MapDetails { get; set; }

        public IndexingPerformanceOperation[] Operations { get; set; }
    }
}