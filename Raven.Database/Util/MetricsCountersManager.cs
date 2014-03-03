// -----------------------------------------------------------------------
//  <copyright file="MetricsCountersManager.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.ComponentModel;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Security;
using System.Threading;
using System.Threading.Tasks;
using metrics;
using metrics.Core;
using Raven.Abstractions.Logging;
namespace Raven.Database.Util
{
    public class MetricsCountersManager : IDisposable
    {
        readonly Metrics dbMetrics = new Metrics();

<<<<<<< HEAD
=======
        public HistogramMetric RequestDuationMetric { get; private set; }
>>>>>>> upstream/new3
        public PerSecondCounterMetric DocsPerSecond { get; private set; }

        public PerSecondCounterMetric IndexedPerSecond { get; private set; }

        public PerSecondCounterMetric ReducedPerSecond { get; private set; }

        public MeterMetric ConcurrentRequests { get; private set; }

        public MeterMetric RequestsMeter { get; private set; }
        public PerSecondCounterMetric RequestsPerSecondCounter { get; private set; }

        public MetricsCountersManager()
        {
            ConcurrentRequests = dbMetrics.Meter("metrics", "req/sec", "Concurrent Requests Meter", TimeUnit.Seconds);
<<<<<<< HEAD
            
=======

            RequestDuationMetric = dbMetrics.Histogram("metrics", "req duration");

>>>>>>> upstream/new3
            DocsPerSecond = dbMetrics.TimedCounter("metrics", "docs/sec", "Docs Per Second Counter");
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
