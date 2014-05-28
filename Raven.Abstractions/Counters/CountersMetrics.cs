using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Abstractions.Data;

namespace Raven.Abstractions.Counters
{
    public class CountersMetrics
    {
        public double RequestsPerSecond { get; set; }

        public MeterData Resets { get; set; }
        public MeterData Increments { get; set; }
        public MeterData Decrements { get; set; }
        public MeterData ClientRuqeusts { get; set; }
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
}
