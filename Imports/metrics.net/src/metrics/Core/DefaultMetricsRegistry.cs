using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using Metrics.MetricData;

namespace Metrics.Core
{
    public sealed class DefaultMetricsRegistry : MetricsRegistry
    {
        private class MetricMetaCatalog<TMetric> where TMetric:ResetableMetric
        {
            private readonly ConcurrentDictionary<string, TMetric> metrics =
                new ConcurrentDictionary<string, TMetric>();

            public IEnumerable<TMetric> All
            {
                get
                {
                    return this.metrics.Values.OrderBy(m => m.Name);
                }
            }

            public TMetric GetOrAdd(string name, Func<string,TMetric> metricProvider)
            {
                return this.metrics.GetOrAdd(name, metricProvider);
            }

            public void Clear()
            {
                var values = this.metrics.Values;
                this.metrics.Clear();
                foreach (var value in values)
                {
                    using (value as IDisposable) { }
                }
            }

            public void Reset()
            {
                foreach (var metric in this.metrics.Values)
                {
                    var resetable = metric as ResetableMetric;
                    if (resetable != null)
                    {
                        resetable.Reset();
                    }
                }
            }
        }

        private readonly MetricMetaCatalog<Meter> meters = new MetricMetaCatalog<Meter>();
        private readonly MetricMetaCatalog<Meter> perSecondMeters = new MetricMetaCatalog<Meter>();
        private readonly MetricMetaCatalog<Histogram> histograms =
            new MetricMetaCatalog<Histogram>();

        private readonly MetricMetaCatalog<Meter> bufferedAverageMeters = new MetricMetaCatalog<Meter>();

        public DefaultMetricsRegistry()
        {
            this.DataProvider = new DefaultRegistryDataProvider(() => this.meters.All, () => this.histograms.All);
        }

        public RegistryDataProvider DataProvider { get; private set; }
        

        public Meter Meter(string name, Func<string,Meter> builder)
        {
            return this.meters.GetOrAdd(name, builder);
        }

        public Histogram Histogram(string name, Func<string, Histogram> builder)
        {
            return this.histograms.GetOrAdd(name, builder);
        }

        public Meter PerSecondMetric(string name, Func<string, Meter> builder)
        {
            return this.perSecondMeters.GetOrAdd(name, builder);
        }
        public Meter BufferedAverageMeter(string name, Func<object, Meter> builder)
        {
            return this.bufferedAverageMeters.GetOrAdd(name, builder);
        }

        public void ClearAllMetrics()
        {
            this.meters.Clear();
            this.histograms.Clear();
        }

        public void ResetMetricsValues()
        {
            this.meters.Reset();
            this.histograms.Reset();
        }
    }
}
