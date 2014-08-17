using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using metrics;
using metrics.Core;
using Raven.Bundles.Replication.Tasks;

namespace Raven.Database.Counters
{
    public class CountersMetricsManager
    {
        readonly Metrics counterMetrics = new Metrics();
        public PerSecondCounterMetric RequestsPerSecondCounter { get; private set; }

        public MeterMetric Resets { get; private set; }
        public MeterMetric Increments { get; private set; }
        public MeterMetric Decrements { get; private set; }
        public MeterMetric ClientRuqeusts { get; private set; }
        public MeterMetric IncomingReplications { get; private set; }
        public MeterMetric OutgoingReplications { get; private set; }

        public HistogramMetric IncSizeMetrics { get; private set; }
        public HistogramMetric DecSizeMetrics { get; private set; }
        public HistogramMetric RequestDuationMetric { get; private set; }
        
        public ConcurrentDictionary<string, MeterMetric> ReplicationBatchSizeMeter { get; private set; }
        public ConcurrentDictionary<string, HistogramMetric> ReplicationBatchSizeHistogram { get; private set; }
        public ConcurrentDictionary<string, HistogramMetric> ReplicationDurationHistogram { get; private set; }

        public CountersMetricsManager()
        {
            Resets = counterMetrics.Meter("counterMetrics", "reset/min", "resets", TimeUnit.Minutes);
            Increments = counterMetrics.Meter("counterMetrics", "inc/min", "increments", TimeUnit.Minutes);
            Decrements = counterMetrics.Meter("counterMetrics", "dec/min", "decrements", TimeUnit.Minutes);
            ClientRuqeusts = counterMetrics.Meter("counterMetrics", "client/min", "client requests", TimeUnit.Minutes);

            IncomingReplications = counterMetrics.Meter("counterMetrics", "RepIn/min", "replicaitons", TimeUnit.Minutes);
            OutgoingReplications = counterMetrics.Meter("counterMetrics", "RepOut/min", "replicaitons", TimeUnit.Minutes);



            RequestsPerSecondCounter = counterMetrics.TimedCounter("counterMetrics", "req/sec counter", "Requests Per Second");

            IncSizeMetrics = counterMetrics.Histogram("counterMetrics", "inc delta sizes");
            DecSizeMetrics = counterMetrics.Histogram("counterMetrics", "dec delta sizes");
            RequestDuationMetric = counterMetrics.Histogram("counterMetrics", "inc/dec request durations");
            RequestDuationMetric = counterMetrics.Histogram("counterMetrics", "inc/dec request durations");
            
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
        }

        public MeterMetric GetReplicationBatchSizeMetric(string serverUrl)
        {
            return ReplicationBatchSizeMeter.GetOrAdd(serverUrl,
                s => counterMetrics.Meter("counterMetrics", "counters replication/min for: "+ s, "Replication docs/min Counter", TimeUnit.Minutes));
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
