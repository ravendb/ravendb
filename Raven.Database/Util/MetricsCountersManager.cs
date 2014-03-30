// -----------------------------------------------------------------------
//  <copyright file="MetricsCountersManager.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using metrics;
using metrics.Core;

namespace Raven.Database.Util
{
    public class MetricsCountersManager : IDisposable
    {
        readonly Metrics dbMetrics = new Metrics();

        public HistogramMetric RequestDuationMetric { get; private set; }
        public PerSecondCounterMetric DocsPerSecond { get; private set; }
        public PerSecondCounterMetric FilesPerSecond { get; private set; }

        public PerSecondCounterMetric IndexedPerSecond { get; private set; }

        public PerSecondCounterMetric ReducedPerSecond { get; private set; }

        public MeterMetric ConcurrentRequests { get; private set; }

        public MeterMetric RequestsMeter { get; private set; }
        public PerSecondCounterMetric RequestsPerSecondCounter { get; private set; }

        public MetricsCountersManager()
        {
            ConcurrentRequests = dbMetrics.Meter("metrics", "req/sec", "Concurrent Requests Meter", TimeUnit.Seconds);

            RequestDuationMetric = dbMetrics.Histogram("metrics", "req duration");

            DocsPerSecond = dbMetrics.TimedCounter("metrics", "docs/sec", "Docs Per Second Counter");
            FilesPerSecond = dbMetrics.TimedCounter("metrics", "files/sec", "Files Per Second Counter");
            RequestsPerSecondCounter = dbMetrics.TimedCounter("metrics", "req/sec counter", "Requests Per Second");
            ReducedPerSecond = dbMetrics.TimedCounter("metrics", "reduces/sec", "Reduced Per Second Counter");
            IndexedPerSecond = dbMetrics.TimedCounter("metrics", "indexed/sec", "Index Per Second Counter");
        }

        public void Dispose()
        {
            dbMetrics.Dispose();
        }
    }
}