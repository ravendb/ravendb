using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using Raven.Imports.metrics;
using Raven.Imports.metrics.Core;

namespace Raven.Database.TimeSeries
{
    [CLSCompliant(false)]
    public class TimeSeriesMetricsManager
    {
        readonly Metrics timeSeriesMetrics = new Metrics();
        public PerSecondCounterMetric RequestsPerSecondCounter { get; private set; }

        public MeterMetric Appends { get; private set; }
        public MeterMetric Deletes { get; private set; }
        public MeterMetric ClientRequests { get; private set; }
        public MeterMetric IncomingReplications { get; private set; }
        public MeterMetric OutgoingReplications { get; private set; }

        public HistogramMetric RequestDurationMetric { get; private set; }
        
        public ConcurrentDictionary<string, MeterMetric> ReplicationBatchSizeMeter { get; private set; }
        public ConcurrentDictionary<string, HistogramMetric> ReplicationBatchSizeHistogram { get; private set; }
        public ConcurrentDictionary<string, HistogramMetric> ReplicationDurationHistogram { get; private set; }

        public long ConcurrentRequestsCount;

        public TimeSeriesMetricsManager()
        {
            Appends = timeSeriesMetrics.Meter("timeSeriesMetrics", "append/min", "appends", TimeUnit.Minutes);
            Deletes = timeSeriesMetrics.Meter("timeSeriesMetrics", "delete/min", "deletes", TimeUnit.Minutes);
            ClientRequests = timeSeriesMetrics.Meter("timeSeriesMetrics", "client/min", "client requests", TimeUnit.Minutes);

            IncomingReplications = timeSeriesMetrics.Meter("timeSeriesMetrics", "RepIn/min", "replications", TimeUnit.Minutes);
            OutgoingReplications = timeSeriesMetrics.Meter("timeSeriesMetrics", "RepOut/min", "replications", TimeUnit.Minutes);

            RequestsPerSecondCounter = timeSeriesMetrics.TimedCounter("timeSeriesMetrics", "req/sec time series", "Requests Per Second");

            RequestDurationMetric = timeSeriesMetrics.Histogram("timeSeriesMetrics", "inc/dec request durations");
            
            ReplicationBatchSizeMeter = new ConcurrentDictionary<string, MeterMetric>();
            ReplicationBatchSizeHistogram = new ConcurrentDictionary<string, HistogramMetric>();
            ReplicationDurationHistogram = new ConcurrentDictionary<string, HistogramMetric>();
        }


        // Maybe use Gauges for metrics of particular timeSeriess, on demand?
        public void AddGauge<T>(Type type, string name, Func<T> function)
        {
            timeSeriesMetrics.Gauge(type, name, function);
        }

        public Dictionary<string, Dictionary<string, string>> Gauges
        {
            get
            {
                return timeSeriesMetrics
                    .All
                    .Where(x => x.Value is GaugeMetric)
                    .GroupBy(x => x.Key.Context)
                    .ToDictionary(x => x.Key, x => x.ToDictionary(y => y.Key.Name, y => ((GaugeMetric)y.Value).ValueAsString));
            }
        }

        public void Dispose()
        {
            timeSeriesMetrics.Dispose();
        }

        public MeterMetric GetReplicationBatchSizeMetric(string serverUrl)
        {
            return ReplicationBatchSizeMeter.GetOrAdd(serverUrl,
                s => timeSeriesMetrics.Meter("timeSeriesMetrics", "timeSeriess replication/min for: "+ s, "Replication docs/min TimeSeries", TimeUnit.Minutes));
        }

        public HistogramMetric GetReplicationBatchSizeHistogram(string serverUrl)
        {
            return ReplicationBatchSizeHistogram.GetOrAdd(serverUrl,
                s => timeSeriesMetrics.Histogram("timeSeriesMetrics", "Time Series Replication docs/min Histogram for : " + s));
        }

        public HistogramMetric GetReplicationDurationHistogram(string serverUrl)
        {
            return ReplicationDurationHistogram.GetOrAdd(serverUrl,
                s => timeSeriesMetrics.Histogram("timeSeriesMetrics", "Time Series Replication duration Histogram for: " + s));
        }
    }
}
