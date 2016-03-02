using System;
using System.Threading;
using Metrics.MetricData;
using Metrics.Sampling;
using Metrics.Utils;

namespace Metrics.Core
{
    public sealed class HistogramMetric : Histogram
    {
        private readonly Reservoir reservoir;
        private long last;

        public HistogramMetric(string name, Scheduler scheduler)
        {
            Name = name;
            this.reservoir = new ExponentiallyDecayingReservoir(scheduler);
        }

        public void Update(long value)
        {
            this.last = value;
            
            this.reservoir.Update(value);
        }

        public HistogramValue GetValue(bool resetMetric = false)
        {
            var value = new HistogramValue(Name,this.last, this.reservoir.GetSnapshot(resetMetric));
            if (resetMetric)
            {
                Interlocked.Exchange(ref this.last, 0);
            }
            return value;
        }

        public HistogramValue Value
        {
            get
            {
                return GetValue();
            }
        }

        public string Name { get; private set; }

        public void Reset()
        {
            Interlocked.Exchange(ref this.last, 0);
            this.last=0;
            this.reservoir.Reset();
        }
    }
}
