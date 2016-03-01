using System;
using System.Collections.Generic;
using Metrics.MetricData;

namespace Metrics.Core
{
    public sealed class NullMetricsRegistry : MetricsRegistry
    {
        private struct NullMetric : Meter, Histogram, RegistryDataProvider
        {
            public string Name { get; set; }
            public static readonly NullMetric Instance = new NullMetric();
            public void Mark() { }
            public void Mark(long count) { }
            public void Mark(string item) { }
            public void Mark(string item, long count) { }
            public void Update(long value) { }
            public void Reset() { }
            public IEnumerable<Meter> Meters { get { yield break; } }
            public IEnumerable<Histogram> Histograms { get { yield break; } }
        }

        public RegistryDataProvider DataProvider { get { return NullMetric.Instance; } }

        public Meter PerSecondMetric(string name, Func<string, Meter> builder)
        {
            return NullMetric.Instance;
        }

        public void ClearAllMetrics() { }
        public void ResetMetricsValues() { }
        public Meter BufferedAverageMeter(string name, Func<object, Meter> func)
        {
            return NullMetric.Instance;
        }

        public Meter Meter(string name, Func<string,Meter> builder) 
        {
            return NullMetric.Instance;
        }

        public Histogram Histogram(string name, Func<string,Histogram> builder) 
        {
            return NullMetric.Instance;
        }
    }
}
