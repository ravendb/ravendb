using System;
using System.Collections.Generic;
using Raven.Abstractions.Data;

namespace Raven.Abstractions.Counters
{
    public class CounterStorageStats
    {
        public string Name { get; set; }

        public string Url { get; set; }

        public long CountersCount { get; set; }

        public long GroupsCount { get; set; }

        public long TombstonesCount { get; set; }

        public long LastCounterEtag { get; set; }

        public int ReplicationTasksCount { get; set; }

        public string CounterStorageSize { get; set; }

        public long ReplicatedServersCount { get; set; }

        public double RequestsPerSecond { get; set; }
    }

    public class CountersStorageMetrics
    {
        public double RequestsPerSecond { get; set; }

        public MeterData Resets { get; set; }

        public MeterData Increments { get; set; }

        public MeterData Decrements { get; set; }

        public MeterData ClientRequests { get; set; }

        public MeterData IncomingReplications { get; set; }

        public MeterData OutgoingReplications { get; set; }

        public HistogramData RequestsDuration { get; set; }

        public HistogramData IncSizes { get; set; }

        public HistogramData DecSizes { get; set; }

        public Dictionary<string, MeterData> ReplicationBatchSizeMeter { get; set; }

        public Dictionary<string, MeterData> ReplicationDurationMeter { get; set; }

        public Dictionary<string, HistogramData> ReplicationBatchSizeHistogram { get; set; }

        public Dictionary<string, HistogramData> ReplicationDurationHistogram { get; set; }
    }

    public class CounterStorageReplicationStats
    {
        public List<CounterDestinationStats> Stats { get; set; }
    }

    public class CounterDestinationStats
    {
        public int FailureCountInternal = 0;

        public string Url { get; set; }

        public DateTime? LastHeartbeatReceived { get; set; }

        public long LastReplicatedEtag { get; set; }

        public DateTime? LastReplicatedLastModified { get; set; }

        public DateTime? LastSuccessTimestamp { get; set; }

        public DateTime? LastFailureTimestamp { get; set; }

        public int FailureCount { get { return FailureCountInternal; } }

        public string LastError { get; set; }
    }
}
