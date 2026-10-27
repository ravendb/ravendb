using System;
using System.Collections.Generic;
using Sparrow.Json.Parsing;

namespace Raven.Server.NotificationCenter.Notifications.Details
{
    public sealed class StorageCongestionDetails : INotificationDetails
    {
        public string NodeTag { get; set; }

        public string MountPoint { get; set; }

        public string PathOnDevice { get; set; }

        // plain words, why the disk is considered saturated
        public string Cause { get; set; }

        public DateTime SaturatedSince { get; set; }

        public double DrainShareOfLastHour { get; set; }

        // the device average at the time of the hint update
        public double JournalWriteLatencyInMs { get; set; }

        // median and peak of the last hour, null when no journal write was sampled
        public double? TypicalJournalWriteLatencyInMs { get; set; }

        public double? PeakJournalWriteLatencyInMs { get; set; }

        public double SyncCostInMs { get; set; }

        // false when Storage.IO.Metrics.Enabled is off: no slow write is measured then
        public bool SlowWritesMeasured { get; set; }

        // the worst slow write per file on this disk during the last hour (same shape as the Slow IO hint)
        public Dictionary<string, SlowIoDetails.SlowWriteInfo> Writes { get; set; } = new();

        public DynamicJsonValue ToJson()
        {
            var writes = new DynamicJsonValue();
            foreach (var kvp in Writes)
                writes[kvp.Key] = kvp.Value.ToJson();

            return new DynamicJsonValue
            {
                [nameof(NodeTag)] = NodeTag,
                [nameof(MountPoint)] = MountPoint,
                [nameof(PathOnDevice)] = PathOnDevice,
                [nameof(Cause)] = Cause,
                [nameof(SaturatedSince)] = SaturatedSince,
                [nameof(DrainShareOfLastHour)] = DrainShareOfLastHour,
                [nameof(JournalWriteLatencyInMs)] = JournalWriteLatencyInMs,
                [nameof(TypicalJournalWriteLatencyInMs)] = TypicalJournalWriteLatencyInMs,
                [nameof(PeakJournalWriteLatencyInMs)] = PeakJournalWriteLatencyInMs,
                [nameof(SyncCostInMs)] = SyncCostInMs,
                [nameof(SlowWritesMeasured)] = SlowWritesMeasured,
                [nameof(Writes)] = writes
            };
        }
    }
}
