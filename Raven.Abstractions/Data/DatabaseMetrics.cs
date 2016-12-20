using System;
using System.Collections.Generic;

namespace Raven.Abstractions.Data
{
    public interface IMetricsData
    {
    }

    public class DatabaseMetrics
    {
        public double DocsWritesPerSecond { get; set; }
        public double IndexedPerSecond { get; set; }
        public double ReducedPerSecond { get; set; }
        public double RequestsPerSecond { get; set; }
        public MeterData Requests { get; set; }
        public HistogramData RequestsDuration { get; set; }
        public MeterData JsonDeserializationsPerSecond { get; set; }
        public MeterData JsonDeserializedBytesPerSecond { get; set; }
        public MeterData JsonSerializationsPerSecond { get; set; }
        public MeterData JsonSerializedBytesPerSecond { get; set; }
        public HistogramData StaleIndexMaps { get; set; }
        public HistogramData StaleIndexReduces { get; set; }
        public Dictionary<string, Dictionary<string, string>> Gauges { get; set; }
        public Dictionary<string, MeterData> ReplicationBatchSizeMeter { get; set; }
        public Dictionary<string, HistogramData> ReplicationBatchSizeHistogram { get; set; }
        public Dictionary<string, HistogramData> ReplicationDurationHistogram { get; set; }
    }

    public class HistogramData : IMetricsData
    {
        public HistogramData()
        {
            Percentiles = new Dictionary<string, double>();
        }

        public long Counter { get; set; }
        public double Max { get; set; }
        public double Min { get; set; }
        public double Mean { get; set; }
        public double Stdev { get; set; }
        public Dictionary<string, double> Percentiles { get; set; }
        public MetricType Type = MetricType.Histogram;
    }

    public class MeterData : IMetricsData
    {
        public long Count { get; set; }
        public double MeanRate { get; set; }
        public double OneMinuteRate { get; set; }
        public double FiveMinuteRate { get; set; }
        public double FifteenMinuteRate { get; set; }
        public MetricType Type = MetricType.Meter;
    }

    public enum MetricType
    {
        Meter = 1,
        Histogram = 2,

        //backward compatibility for https://github.com/ravendb/ravendb/commit/57a7a5cbab615f5aaf125b816f2f9c29078ed93e#diff-b8c45e35a3b38ccf9dcccca40f6ac0e9
        [Obsolete("Backward compatibility only. Use Histogram.")]
        Historgram = 2
    }
}
