// -----------------------------------------------------------------------
//  <copyright file="MetricsCountersManager.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Linq;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Text;
using System.Threading.Tasks;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Replication;
using Raven.Bundles.Replication.Tasks;
using Raven.Imports.metrics;
using Raven.Imports.metrics.Core;

namespace Raven.Database.Util
{
    [CLSCompliant(false)]
    public class MetricsCountersManager : IDisposable
    {
        private readonly Metrics dbMetrics = new Metrics();

        public HistogramMetric StaleIndexMaps { get; private set; }

        public HistogramMetric StaleIndexReduces { get; private set; }

        public HistogramMetric RequestDurationMetric { get; private set; }
        public OneMinuteMetric RequestDurationLastMinute { get; set; }

        public PerSecondCounterMetric DocsPerSecond { get; private set; }
        public PerSecondCounterMetric FilesPerSecond { get; private set; }

        public PerSecondCounterMetric IndexedPerSecond { get; private set; }

        public PerSecondCounterMetric ReducedPerSecond { get; private set; }

        public MeterMetric ConcurrentRequests { get; private set; }

        public PerSecondCounterMetric RequestsPerSecondCounter { get; private set; }
        public PerSecondCounterMetric RequestsPerSecondTimeSeries { get; private set; }

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
            MetricsTicker.Instance.AddFiveSecondsIntervalMeterMetric(ConcurrentRequests);

            RequestDurationLastMinute = new OneMinuteMetric();
            MetricsTicker.Instance.AddFifteenSecondsIntervalMeterMetric(RequestDurationLastMinute);

            RequestDurationMetric = dbMetrics.Histogram("metrics", "req duration");

            DocsPerSecond = dbMetrics.TimedCounter("metrics", "docs/sec", "Docs Per Second Counter");
            MetricsTicker.Instance.AddPerSecondCounterMetric(DocsPerSecond);

            FilesPerSecond = dbMetrics.TimedCounter("metrics", "files/sec", "Files Per Second Counter");
            MetricsTicker.Instance.AddPerSecondCounterMetric(FilesPerSecond);

            RequestsPerSecondCounter = dbMetrics.TimedCounter("metrics", "req/sec counter", "Requests Per Second");
            MetricsTicker.Instance.AddPerSecondCounterMetric(RequestsPerSecondCounter);

            RequestsPerSecondTimeSeries = dbMetrics.TimedCounter("metrics", "req/sec counter", "Requests Per Second");
            MetricsTicker.Instance.AddPerSecondCounterMetric(RequestsPerSecondTimeSeries);

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

            MetricsTicker.Instance.RemoveFiveSecondsIntervalMeterMetric(ConcurrentRequests);
            foreach (var batchSizeMeter in ReplicationBatchSizeMeter)
            {
                MetricsTicker.Instance.RemoveFiveSecondsIntervalMeterMetric(batchSizeMeter.Value);
            }

            MetricsTicker.Instance.RemovePerSecondCounterMetric(DocsPerSecond);
            MetricsTicker.Instance.RemovePerSecondCounterMetric(FilesPerSecond);
            MetricsTicker.Instance.RemovePerSecondCounterMetric(RequestsPerSecondCounter);
            MetricsTicker.Instance.RemovePerSecondCounterMetric(RequestsPerSecondTimeSeries);
            MetricsTicker.Instance.RemovePerSecondCounterMetric(ReducedPerSecond);
            MetricsTicker.Instance.RemovePerSecondCounterMetric(IndexedPerSecond);
            MetricsTicker.Instance.RemoveFifteenSecondsIntervalMeterMetric(RequestDurationLastMinute);
        }

        public MeterMetric GetReplicationBatchSizeMetric(ReplicationStrategy destination)
        {
            return ReplicationBatchSizeMeter.GetOrAdd(destination.ConnectionStringOptions.Url,
                s =>
                {
                    var newMetric = dbMetrics.Meter("metrics", "docs/min for " + s, "Replication docs/min Counter", TimeUnit.Minutes);
                    MetricsTicker.Instance.AddFiveSecondsIntervalMeterMetric(newMetric);
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

    public class OneMinuteMetric: ICounterMetric
    {
        private readonly ConcurrentQueue<OneMinuteMetricRecord> records;

        private class OneMinuteMetricRecord
        {
            public long Value { get; set; }

            public DateTime TimeAdded { get; set; }
        }

        public OneMinuteMetric()
        {
            records = new ConcurrentQueue<OneMinuteMetricRecord>();
        }

        private void CleanupQueue()
        {
            var now = SystemTime.UtcNow;
            OneMinuteMetricRecord record;
            while (records.TryPeek(out record))
            {
                if ((now - record.TimeAdded).TotalSeconds < 60)
                    return;

                records.TryDequeue(out record);
            }
        }

        public void AddRecord(long value)
        {
            records.Enqueue(new OneMinuteMetricRecord
            {
                Value = value, 
                TimeAdded = SystemTime.UtcNow
            });
        }

        public OneMinuteMetricData GetData()
        {
            var now = SystemTime.UtcNow;
            var values = records
                .Where(x => (now - x.TimeAdded).TotalSeconds <= 60)
                .ToList();

            var min = 0L;
            var max = 0L;
            var sum = 0L;
            double avg = 0;
            foreach (var value in values.Select(v => v.Value))
            {
                min = Math.Min(min, value);
                max = Math.Max(max, value);
                sum += value;
            }

            if (values.Count > 0)
                avg = sum / (double)values.Count;

            return new OneMinuteMetricData { Count = values.Count, Min = min, Max = max, Avg = avg };
        }

        [IgnoreDataMember]
        public IMetric Copy
        {
            get
            {
              return new OneMinuteMetric();  
            } 
        }

        public void LogJson(StringBuilder sb)
        {
            sb.Append("{\"count\":").Append(GetData().Count).Append("}");
        }

        public void Tick()
        {
            CleanupQueue();
        }
    }
}
