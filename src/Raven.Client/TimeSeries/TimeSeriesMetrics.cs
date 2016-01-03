using System.Collections.Generic;
using Raven.Abstractions.Data;

namespace Raven.Abstractions.TimeSeries
{
    public class TimeSeriesMetrics
    {
        public double RequestsPerSecond { get; set; }
        
        public MeterData Appends { get; set; }
        
        public MeterData Deletes { get; set; }

        public MeterData ClientRequests { get; set; }

        public MeterData IncomingReplications { get; set; }

        public MeterData OutgoingReplications { get; set; }

        public HistogramData RequestsDuration { get; set; }

        public Dictionary<string, MeterData> ReplicationBatchSizeMeter { get; set; }

        public Dictionary<string, MeterData> ReplicationDurationMeter { get; set; }

        public Dictionary<string, HistogramData> ReplicationBatchSizeHistogram { get; set; }

        public Dictionary<string, HistogramData> ReplicationDurationHistogram { get; set; }
    }
}
