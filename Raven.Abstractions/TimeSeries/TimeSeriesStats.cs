using System;
using System.Collections.Generic;
using Raven.Abstractions.Data;

namespace Raven.Abstractions.TimeSeries
{
    public class TimeSeriesStats
    {
        public string Name { get; set; }

        public string Url { get; set; }

        public long TimeSeriesCount { get; set; }

        public string TimeSeriesSize { get; set; }

		public double RequestsPerSecond { get; set; }
    }

    public class TimeSeriesMetrics
    {
        public double RequestsPerSecond { get; set; }

        public MeterData ClientRequests { get; set; }

        public MeterData IncomingReplications { get; set; }

        public MeterData OutgoingReplications { get; set; }

        public HistogramData RequestsDuration { get; set; }

        public Dictionary<string, MeterData> ReplicationBatchSizeMeter { get; set; }

        public Dictionary<string, MeterData> ReplicationDurationMeter { get; set; }

        public Dictionary<string, HistogramData> ReplicationBatchSizeHistogram { get; set; }

        public Dictionary<string, HistogramData> ReplicationDurationHistogram { get; set; }
    }

    public class TimeSeriesReplicationStats
    {
        public List<TimeSeriesDestinationStats> Stats { get; set; }
    }

    public class TimeSeriesDestinationStats
    {
        public int FailureCountInternal = 0;

        public string Url { get; set; }

        public DateTime? LastHeartbeatReceived { get; set; }

        public DateTime? LastReplicatedLastModified { get; set; }

        public DateTime? LastSuccessTimestamp { get; set; }

        public DateTime? LastFailureTimestamp { get; set; }

        public int FailureCount { get { return FailureCountInternal; } }

        public string LastError { get; set; }
    }
}