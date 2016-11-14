using System;

namespace Raven.NewClient.Abstractions.TimeSeries
{
    public class TimeSeriesDestinationStats
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
