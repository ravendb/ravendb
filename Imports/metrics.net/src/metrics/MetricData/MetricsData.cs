
using System;
using System.Collections.Generic;
using System.Linq;

namespace Metrics.MetricData
{
    public sealed class MetricsData
    {
        public static readonly MetricsData Empty = new MetricsData(string.Empty, DateTime.MinValue,
            Enumerable.Empty<Meter>(),
            Enumerable.Empty<Histogram>(),
            Enumerable.Empty<MetricsData>());

        public readonly string Context;
        public readonly DateTime Timestamp;
        
        
        public readonly IEnumerable<Meter> Meters;
        public readonly IEnumerable<Histogram> Histograms;
        public readonly IEnumerable<MetricsData> ChildMetrics;

        public MetricsData(string context, DateTime timestamp,
            IEnumerable<Meter> meters,
            IEnumerable<Histogram> histograms,
            IEnumerable<MetricsData> childMetrics)
        {
            this.Context = context;
            this.Timestamp = timestamp;
            this.Meters = meters;
            this.Histograms = histograms;
            this.ChildMetrics = childMetrics;
        }

        public MetricsData Flatten()
        {
            return new MetricsData(this.Context, this.Timestamp,
                this.Meters.Union(this.ChildMetrics.SelectMany(m => m.Flatten().Meters)),
                this.Histograms.Union(this.ChildMetrics.SelectMany(m => m.Flatten().Histograms)),
                Enumerable.Empty<MetricsData>()
            );
        }
    }
}
