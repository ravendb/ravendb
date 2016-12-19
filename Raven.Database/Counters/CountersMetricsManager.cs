using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using Raven.Database.Util;
using Raven.Imports.metrics;
using Raven.Imports.metrics.Core;

namespace Raven.Database.Counters
{
    [CLSCompliant(false)]
    public class CountersMetricsManager : IDisposable
    {
        readonly Metrics counterMetrics = new Metrics();
        public PerSecondCounterMetric RequestsPerSecondCounter { get; private set; }

        public MeterMetric Increments { get; private set; }
        public MeterMetric Decrements { get; private set; }
        public MeterMetric Resets { get; private set; }
        public MeterMetric Deletes { get; private set; }
        public MeterMetric ClientRequests { get; private set; }
        public MeterMetric IncomingReplications { get; private set; }
        public MeterMetric OutgoingReplications { get; private set; }

        public HistogramMetric IncSizeMetrics { get; private set; }
        public HistogramMetric DecSizeMetrics { get; private set; }
        public HistogramMetric RequestDurationMetric { get; private set; }
        
        public ConcurrentDictionary<string, MeterMetric> ReplicationBatchSizeMeter { get; private set; }
        public ConcurrentDictionary<string, HistogramMetric> ReplicationBatchSizeHistogram { get; private set; }
        public ConcurrentDictionary<string, HistogramMetric> ReplicationDurationHistogram { get; private set; }

        public long ConcurrentRequestsCount;

        public CountersMetricsManager()
        {
            Increments = counterMetrics.Meter("counterMetrics", "inc/min", "increments", TimeUnit.Minutes);
            MetricsTicker.Instance.AddFiveSecondsIntervalMeterMetric(Increments);

            Decrements = counterMetrics.Meter("counterMetrics", "dec/min", "decrements", TimeUnit.Minutes);
            MetricsTicker.Instance.AddFiveSecondsIntervalMeterMetric(Decrements);

            Resets = counterMetrics.Meter("counterMetrics", "reset/min", "resets", TimeUnit.Minutes);
            MetricsTicker.Instance.AddFiveSecondsIntervalMeterMetric(Resets);

            Deletes = counterMetrics.Meter("counterMetrics", "delete/min", "deletes", TimeUnit.Minutes);
            MetricsTicker.Instance.AddFiveSecondsIntervalMeterMetric(Deletes);

            ClientRequests = counterMetrics.Meter("counterMetrics", "client/min", "client requests", TimeUnit.Minutes);
            MetricsTicker.Instance.AddFiveSecondsIntervalMeterMetric(ClientRequests);

            IncomingReplications = counterMetrics.Meter("counterMetrics", "RepIn/min", "replications", TimeUnit.Minutes);
            MetricsTicker.Instance.AddFiveSecondsIntervalMeterMetric(IncomingReplications);

            OutgoingReplications = counterMetrics.Meter("counterMetrics", "RepOut/min", "replications", TimeUnit.Minutes);
            MetricsTicker.Instance.AddFiveSecondsIntervalMeterMetric(OutgoingReplications);

            RequestsPerSecondCounter = counterMetrics.TimedCounter("counterMetrics", "req/sec counter", "Requests Per Second");
            MetricsTicker.Instance.AddPerSecondCounterMetric(RequestsPerSecondCounter);

            IncSizeMetrics = counterMetrics.Histogram("counterMetrics", "inc delta sizes");
            DecSizeMetrics = counterMetrics.Histogram("counterMetrics", "dec delta sizes");
            RequestDurationMetric = counterMetrics.Histogram("counterMetrics", "inc/dec request durations");
            
            ReplicationBatchSizeMeter = new ConcurrentDictionary<string, MeterMetric>();
            ReplicationBatchSizeHistogram = new ConcurrentDictionary<string, HistogramMetric>();
            ReplicationDurationHistogram = new ConcurrentDictionary<string, HistogramMetric>();
        }


        // Maybe use Gauges for metrics of particular counters, on demand?
        public void AddGauge<T>(Type type, string name, Func<T> function)
        {
            counterMetrics.Gauge(type, name, function);
        }

        public Dictionary<string, Dictionary<string, string>> Gauges
        {
            get
            {
                return counterMetrics
                    .All
                    .Where(x => x.Value is GaugeMetric)
                    .GroupBy(x => x.Key.Context)
                    .ToDictionary(x => x.Key, x => x.ToDictionary(y => y.Key.Name, y => ((GaugeMetric)y.Value).ValueAsString));
            }
        }

        public void Dispose()
        {
            counterMetrics.Dispose();

            MetricsTicker.Instance.RemoveFiveSecondsIntervalMeterMetric(Increments);
            MetricsTicker.Instance.RemoveFiveSecondsIntervalMeterMetric(Decrements);
            MetricsTicker.Instance.RemoveFiveSecondsIntervalMeterMetric(Resets);
            MetricsTicker.Instance.RemoveFiveSecondsIntervalMeterMetric(Deletes);
            MetricsTicker.Instance.RemoveFiveSecondsIntervalMeterMetric(ClientRequests);
            MetricsTicker.Instance.RemoveFiveSecondsIntervalMeterMetric(IncomingReplications);
            MetricsTicker.Instance.RemoveFiveSecondsIntervalMeterMetric(OutgoingReplications);
            MetricsTicker.Instance.RemovePerSecondCounterMetric(RequestsPerSecondCounter);

            foreach (var batchSizeMeter in ReplicationBatchSizeMeter)
            {
                MetricsTicker.Instance.RemoveFiveSecondsIntervalMeterMetric(batchSizeMeter.Value);
            }
        }

        public MeterMetric GetReplicationBatchSizeMetric(string serverUrl)
        {
            return ReplicationBatchSizeMeter.GetOrAdd(serverUrl,
                s =>
                {
                    var meter = counterMetrics.Meter("counterMetrics", "counters replication/min for: " + s, "Replication docs/min Counter", TimeUnit.Minutes);
                    MetricsTicker.Instance.AddFiveSecondsIntervalMeterMetric(meter);
                    return meter;
                });
        }

        public HistogramMetric GetReplicationBatchSizeHistogram(string serverUrl)
        {
            return ReplicationBatchSizeHistogram.GetOrAdd(serverUrl,
                s => counterMetrics.Histogram("counterMetrics", "Counter Replication docs/min Histogram for : " + s));
        }

        public HistogramMetric GetReplicationDurationHistogram(string serverUrl)
        {
            return ReplicationDurationHistogram.GetOrAdd(serverUrl,
                s => counterMetrics.Histogram("counterMetrics", "Counter Replication duration Histogram for: " + s));
        }
    }
}
