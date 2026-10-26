using System.Linq;
using Sparrow.Json.Parsing;

namespace Raven.Server.Storage
{
    /// <summary>
    /// The disk status of a node for the cluster dashboard: the worst disk the node writes to, in plain words.
    /// </summary>
    public sealed class StorageStatusInfo : IDynamicJson
    {
        public StorageDeviceStatus Status { get; set; }

        public string Summary { get; set; }

        public string MountPoint { get; set; }

        public double DrainShareOfLastHour { get; set; }

        public double JournalWriteLatencyInMs { get; set; }

        public double SyncCostInMs { get; set; }

        public int Disks { get; set; }

        public static StorageStatusInfo Create(StorageDeviceMonitor monitor)
        {
            CongestionThresholds thresholds = monitor.Thresholds;
            var disks = monitor.Trackers
                .Select(t => (Tracker: t, Status: t.GetStatus(thresholds)))
                .OrderByDescending(x => x.Status)
                .ThenByDescending(x => x.Tracker.Device.JournalWriteLatency)
                .ToList();

            if (disks.Count == 0)
                return null;

            var (worst, status) = disks[0];
            double drainShare = worst.DrainShareOfLastHour;
            double latency = worst.Device.JournalWriteLatency.TotalMilliseconds;

            return new StorageStatusInfo
            {
                Status = status,
                MountPoint = worst.MountPoint,
                DrainShareOfLastHour = drainShare,
                JournalWriteLatencyInMs = latency,
                SyncCostInMs = worst.Device.SyncCost.TotalMilliseconds,
                Disks = disks.Count,
                Summary = status switch
                {
                    StorageDeviceStatus.Saturated => $"Disk {worst.MountPoint} is saturated: {worst.SaturationCause}. Writes and indexing are slowed down.",
                    StorageDeviceStatus.Busy => worst.Device.IsDraining
                        ? $"Disk {worst.MountPoint} is busy right now: the server paces its writes so the disk can keep up."
                        : $"Disk {worst.MountPoint} is busy right now: journal writes take {latency:#,#0.##} ms.",
                    _ => disks.Count == 1 ? "The disk keeps up with the writes." : $"All {disks.Count} disks keep up with the writes."
                }
            };
        }

        public DynamicJsonValue ToJson() => new()
        {
            [nameof(Status)] = Status,
            [nameof(Summary)] = Summary,
            [nameof(MountPoint)] = MountPoint,
            [nameof(DrainShareOfLastHour)] = DrainShareOfLastHour,
            [nameof(JournalWriteLatencyInMs)] = JournalWriteLatencyInMs,
            [nameof(SyncCostInMs)] = SyncCostInMs,
            [nameof(Disks)] = Disks
        };
    }
}
