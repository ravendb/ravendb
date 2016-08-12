// -----------------------------------------------------------------------
//  <copyright file="MetricsCountersManager.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using Raven.Abstractions.Replication;
using Raven.Bundles.Replication.Tasks;
using metrics;
using metrics.Core;
using System.Linq;
using Raven.Abstractions.Util;

namespace Raven.Database.Util
{
    [CLSCompliant(false)]
    public class MetricsCountersManager : IDisposable
    {
        readonly Metrics dbMetrics = new Metrics();

        public HistogramMetric StaleIndexMaps { get; private set; }

        public HistogramMetric StaleIndexReduces { get; private set; }

        public HistogramMetric RequestDuationMetric { get; private set; }
        public PerSecondCounterMetric DocsPerSecond { get; private set; }
        public PerSecondCounterMetric FilesPerSecond { get; private set; }

        public PerSecondCounterMetric IndexedPerSecond { get; private set; }

        public PerSecondCounterMetric ReducedPerSecond { get; private set; }

        public MeterMetric ConcurrentRequests { get; private set; }

        public PerSecondCounterMetric RequestsPerSecondCounter { get; private set; }

        public ConcurrentDictionary<string, MeterMetric> ReplicationBatchSizeMeter { get; private set; }
        
        public ConcurrentDictionary<string, HistogramMetric> ReplicationBatchSizeHistogram { get; private set; }

        public ConcurrentDictionary<string, HistogramMetric> ReplicationDurationHistogram { get; private set; }

        public ConcurrentDictionary<string, ConcurrentQueue<ReplicationPerformanceStats>> ReplicationPerformanceStats { get; private set; }

        public long ConcurrentRequestsCount;

        public MetricsCountersManager()
        {
            StaleIndexMaps = dbMetrics.Histogram("metrics", "stale index maps");
            StaleIndexReduces = dbMetrics.Histogram("metrics", "stale index reduces");

            ConcurrentRequests = dbMetrics.Meter("metrics", "req/sec", "Concurrent Requests Meter", TimeUnit.Seconds);
            MetricsTicker.Instance.AddMeterMetric(ConcurrentRequests);

            RequestDuationMetric = dbMetrics.Histogram("metrics", "req duration");

            DocsPerSecond = dbMetrics.TimedCounter("metrics", "docs/sec", "Docs Per Second Counter");
            MetricsTicker.Instance.AddPerSecondCounterMetric(DocsPerSecond);

            FilesPerSecond = dbMetrics.TimedCounter("metrics", "files/sec", "Files Per Second Counter");
            MetricsTicker.Instance.AddPerSecondCounterMetric(FilesPerSecond);

            RequestsPerSecondCounter = dbMetrics.TimedCounter("metrics", "req/sec counter", "Requests Per Second");
            MetricsTicker.Instance.AddPerSecondCounterMetric(RequestsPerSecondCounter);

            ReducedPerSecond = dbMetrics.TimedCounter("metrics", "reduces/sec", "Reduced Per Second Counter");
            MetricsTicker.Instance.AddPerSecondCounterMetric(ReducedPerSecond);

            IndexedPerSecond = dbMetrics.TimedCounter("metrics", "indexed/sec", "Index Per Second Counter");
            MetricsTicker.Instance.AddPerSecondCounterMetric(IndexedPerSecond);

            ReplicationBatchSizeMeter = new ConcurrentDictionary<string, MeterMetric>();
            ReplicationBatchSizeHistogram = new ConcurrentDictionary<string, HistogramMetric>();
            ReplicationDurationHistogram = new ConcurrentDictionary<string, HistogramMetric>();
            ReplicationPerformanceStats = new ConcurrentDictionary<string, ConcurrentQueue<ReplicationPerformanceStats>>();
        }

        public void AddGauge<T>(Type type, string name, Func<T> function)
        {
            dbMetrics.Gauge(type, name, function);
        }

        public Metrics DbMetrics
        {
            get { return dbMetrics; }
        }

        public Dictionary<string, Dictionary<string, string>> Gauges
        {
            get
            {
                return dbMetrics
                    .All
                    .Where(x => x.Value is GaugeMetric)
                    .GroupBy(x => x.Key.Context)
                    .ToDictionary(x => x.Key, x => x.ToDictionary(y => y.Key.Name, y => ((GaugeMetric)y.Value).ValueAsString));
            }
        }

        public void Dispose()
        {
            dbMetrics.Dispose();

            MetricsTicker.Instance.RemoveMeterMetric(ConcurrentRequests);
            foreach (var batchSizeMeter in ReplicationBatchSizeMeter)
            {
                MetricsTicker.Instance.RemoveMeterMetric(batchSizeMeter.Value);
            }

            MetricsTicker.Instance.RemovePerSecondCounterMetric(DocsPerSecond);
            MetricsTicker.Instance.RemovePerSecondCounterMetric(FilesPerSecond);
            MetricsTicker.Instance.RemovePerSecondCounterMetric(RequestsPerSecondCounter);
            MetricsTicker.Instance.RemovePerSecondCounterMetric(ReducedPerSecond);
            MetricsTicker.Instance.RemovePerSecondCounterMetric(IndexedPerSecond);
        }

        public MeterMetric GetReplicationBatchSizeMetric(ReplicationStrategy destination)
        {
            return ReplicationBatchSizeMeter.GetOrAdd(destination.ConnectionStringOptions.Url,
                s =>
                {
                    var newMetric = dbMetrics.Meter("metrics", "docs/min for " + s, "Replication docs/min Counter", TimeUnit.Minutes);
                    MetricsTicker.Instance.AddMeterMetric(newMetric);
                    return newMetric;
                });
        }

        public HistogramMetric GetReplicationBatchSizeHistogram(ReplicationStrategy destination)
        {
            return ReplicationBatchSizeHistogram.GetOrAdd(destination.ConnectionStringOptions.Url,
                s => dbMetrics.Histogram("metrics", "Replication docs/min Histogram for " + s));
        }

        public HistogramMetric GetReplicationDurationHistogram(ReplicationStrategy destination)
        {
            return ReplicationDurationHistogram.GetOrAdd(destination.ConnectionStringOptions.Url,
                s => dbMetrics.Histogram("metrics", "Replication duration Histogram for " + s));
        }

        public ConcurrentQueue<ReplicationPerformanceStats> GetReplicationPerformanceStats(ReplicationStrategy destination)
        {
            return ReplicationPerformanceStats.GetOrAdd(destination.ConnectionStringOptions.Url,
                                                        s => new ConcurrentQueue<ReplicationPerformanceStats>());
        }
    }
}
