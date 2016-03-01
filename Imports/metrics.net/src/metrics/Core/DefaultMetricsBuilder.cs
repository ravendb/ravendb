
using System;
using Metrics.MetricData;
using Metrics.Sampling;
using Metrics.Utils;

namespace Metrics.Core
{
    public sealed class DefaultMetricsBuilder : MetricsBuilder
    {
        private ActionScheduler _scheduler;

        public DefaultMetricsBuilder()
        {
            _scheduler = new ActionScheduler(Clock.NANOSECONDS_IN_SECOND);
        }
        public Meter BuildMeter(string name)
        {
            return new MeterMetric(name, _scheduler);
        }

        public Meter BuildPerSecondMeter(string name)
        {
            return new PerSecondMetric(name, _scheduler);
        }

        public Meter BuildBufferenAverageMeter(string name, int bufferSize = 10, int intervalInSeconds = 1)
        {
            return new BufferedAverageMeter(name,_scheduler,bufferSize,intervalInSeconds);
        }

        public Histogram BuildHistogram(string name)
        {
            return new HistogramMetric(name,_scheduler);
        }

        public PerSecondMetric BuildPerSecondMetric(string name)
        {
            return new PerSecondMetric(name,_scheduler);
        }
        
    }
}
