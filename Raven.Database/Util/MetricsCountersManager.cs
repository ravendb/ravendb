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
    public class MetricsCountersManager:IDisposable
    {
        private static readonly ILog log = LogManager.GetCurrentClassLogger();

        /// <summary>
        /// The performance counter category name for RavenDB counters.
        /// </summary>
        public const string CategoryName = "RavenDB 2.0";

        // REVIEW: Is this long enough to determine if it *would* hang forever?
        private static readonly TimeSpan PerformanceCounterWaitTimeout = TimeSpan.FromSeconds(3);
        private static bool corruptedCounters;
        readonly Metrics dbMetrics = new Metrics();

        public MetricsCountersManager()
        {

          
        }

         public CounterMetric DocsPerSecond { get; private set; }

         public CounterMetric IndexedPerSecond { get; private set; }

         public CounterMetric ReducedPerSecond { get; private set; }

         public MeterMetric ConcurrentRequests { get; private set; }

        public MeterMetric RequestsMeter { get; private set; }
        public PerSecondCounterMetric RequestsPerSecondCounter { get; private set; }
        public HistogramMetric RequestsPerSecondHistogram { get; private set; }

        public void Setup(string name)
        {
            try
            {
                InstallCounters(name);
              
            }
            catch (UnauthorizedAccessException e)
            {
                log.WarnException("Could not setup performance counters properly because of access permissions, perf counters will not be used", e);
            }
            catch (SecurityException e)
            {
                log.WarnException("Could not setup performance counters properly because of access permissions, perf counters will not be used", e);
            }
            catch (Exception e)
            {
                log.WarnException("Could not setup performance counters properly. Perf counters will not be used.", e);
            }
        }

        private void InstallCounters(string name)
        {
           // RequestsMeter = dbMetrics.Meter(name, "req/sec", "Requests Meter", TimeUnit.Seconds);
            ConcurrentRequests = dbMetrics.Meter(name, "req/sec", "Concurrent Requests Meter", TimeUnit.Seconds);
            RequestsPerSecondCounter = dbMetrics.TimedCounter(name, "req/sec counter", "Requests Per Second");

            DocsPerSecond = dbMetrics.Counter(name, "Docs Per Second Counter");
            RequestsPerSecondHistogram = dbMetrics.Histogram(name, "Request Per Second Histogram");
            ReducedPerSecond = dbMetrics.Counter(name, "Reduced Per Second Counter");
            IndexedPerSecond=dbMetrics.Counter(name,"Index Per Second Counter");
   
 

        }
        public void Dispose()
        {
            ConcurrentRequests.Dispose();
            RequestsPerSecondCounter.Dispose();       
           
        }
    }
}
