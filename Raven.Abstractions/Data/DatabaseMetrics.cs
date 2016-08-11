using System;
using System.Collections.Generic;
using Raven.Abstractions.Util.MiniMetrics;

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
        public OneMinuteMetricData RequestDurationLastMinute { get; set; }
        public HistogramData StaleIndexMaps { get; set; }
        public HistogramData StaleIndexReduces { get; set; }

        public MeterValue? JsonDeserializationsPerSecond { get; set; }
        public MeterValue? JsonDeserializedBytesPerSecond { get; set; }
        public MeterValue? JsonSerializationsPerSecond { get; set; }
        public MeterValue? JsonSerializedBytesPerSecond { get; set; }

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


    public class OneMinuteMetricData : IMetricsData
    {
        public int Count { get; set; }
        public long Min { get; set; }
        public long Max { get; set; }
        public double Avg { get; set; }
    }

    public enum MetricType
    {
        Meter = 1,
        Histogram = 2
    }
}
