using System;
using System.Collections.Generic;

namespace Raven.Abstractions.Data
{
    public class DatabaseMetrics
    {
        public double DocsWritesPerSecond { get; set; }

        public double IndexedPerSecond { get; set; }

        public double ReducedPerSecond { get; set; }

        public double RequestsPerSecond { get; set; }

        public MeterData Requests { get; set; }

        public HistogramData RequestsDuration { get; set; }

        public HistogramData StaleIndexMaps { get; set; }

        public HistogramData StaleIndexReduces { get; set; }

        public Dictionary<string, Dictionary<string, string>> Gauges { get; set; }

        public Dictionary<string, MeterData> ReplicationBatchSizeMeter { get; set; }

        public Dictionary<string, MeterData> ReplicationDurationMeter { get; set; }

        public Dictionary<string, HistogramData> ReplicationBatchSizeHistogram { get; set; }

        public Dictionary<string, HistogramData> ReplicationDurationHistogram { get; set; }

        public SQLReplicationMetrics SQLReplicationMetrics { get; set; }
    }

    public class SQLReplicationMetrics
    {
        public Dictionary<string, MeterData> SqlReplicationBatchSizeMeter { get;  set; }
        public Dictionary<string, MeterData> SqlReplicationDurationMeter { get;  set; }
        public Dictionary<string, HistogramData> SqlReplicationBatchSizeHistogram { get;  set; }
        public Dictionary<string, HistogramData> SqlReplicationDurationHistogram { get;  set; }
        public Dictionary<Tuple<string, string>, MeterData> SqlReplicationDeleteActionsMeter { get;  set; }
        public Dictionary<Tuple<string, string>, MeterData> SqlReplicationInsertActionsMeter { get;  set; }
        public Dictionary<Tuple<string, string>, MeterData> SqlReplicationDeleteActionsDurationMeter { get;  set; }
        public Dictionary<Tuple<string, string>, MeterData> SqlReplicationInsertActionsDurationMeter { get;  set; }
        public Dictionary<Tuple<string, string>, HistogramData> SqlReplicationDeleteActionsHistogram { get;  set; }
        public Dictionary<Tuple<string, string>, HistogramData> SqlReplicationInsertActionsHistogram { get;  set; }
        public Dictionary<Tuple<string, string>, HistogramData> SqlReplicationDeleteActionsDurationHistogram { get;  set; }
        public Dictionary<Tuple<string, string>, HistogramData> SqlReplicationInsertActionsDurationHistogram { get;  set; }
    }

    public class HistogramData
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
    }

    public class MeterData
    {
        public long Count { get; set; }
        public double MeanRate { get; set; }
        public double OneMinuteRate { get; set; }
        public double FiveMinuteRate { get; set; }
        public double FifteenMinuteRate { get; set; }
    }
}
