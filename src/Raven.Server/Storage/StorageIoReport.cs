using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Server.NotificationCenter.Notifications.Details;
using Sparrow;
using Sparrow.Server.Meters;
using Sparrow.Server.Platform;
using Sparrow.Server.Utils.DiskStatsGetter;
using Voron;
using Voron.Impl.Journal;

namespace Raven.Server.Storage
{
    /// <summary>
    /// Server-wide (Operator) view of how Voron adapts its writes to each disk: per device state, per environment decisions
    /// and shared index journals. Built from in-memory state only, no transactions.
    /// </summary>
    public sealed class StorageIoReport
    {
        public StorageWriteModeInfo WriteMode { get; set; }

        public List<StorageDeviceIoInfo> Devices { get; set; }

        public List<StorageEnvironmentIoInfo> Environments { get; set; }

        public List<SharedJournalsIoInfo> SharedJournals { get; set; }

        public static StorageIoReport Create(StorageDeviceMonitor monitor)
        {
            List<StorageDeviceMonitor.EnvironmentOfDatabase> environments = monitor.GetEnvironments().ToList();

            return new StorageIoReport
            {
                WriteMode = new StorageWriteModeInfo
                {
                    Effective = PalConfiguration.WriteMode,
                    Configured = PalConfiguration.RequestedWriteMode,
                    IoRingQueueSize = PalConfiguration.IoRingQueueSize,
                    LowPriorityIo = PalConfiguration.LowPriorityIo
                },
                Devices = monitor.GetDevices().Keys.Select(d => StorageDeviceIoInfo.Create(d, monitor.GetTracker(d), monitor)).ToList(),
                Environments = environments.Select(StorageEnvironmentIoInfo.Create).ToList(),
                SharedJournals = environments.Where(e => e.Database != null)
                    .GroupBy(e => e.Database)
                    .Select(g => SharedJournalsIoInfo.Create(g.Key, g.ToList()))
                    .Where(x => x != null)
                    .ToList()
            };
        }
    }

    // how data files are written, decided once per process when the PAL starts. Auto uses IoRing when it can be set up,
    // otherwise VectoredFileIo on Linux and FileIo on Windows.
    public sealed class StorageWriteModeInfo
    {
        public Pal.RvnWriteMode Effective { get; set; }

        public Pal.RvnWriteMode Configured { get; set; }

        public int IoRingQueueSize { get; set; }

        public bool LowPriorityIo { get; set; }
    }

    public sealed class StorageDeviceIoInfo
    {
        public string DeviceId { get; set; }

        public string PathOnDevice { get; set; }

        public string MountPoint { get; set; }

        public StorageDeviceStatus Status { get; set; }

        // plain words
        public string StatusReason { get; set; }

        public DeviceWriteBudget.DeviceClass Class { get; set; }

        public string ClassDescription { get; set; }

        public double JournalWriteLatencyInMs { get; set; }

        public long JournalWriteSizeInBytes { get; set; }

        // null when no journal write was recorded since the server started
        public double? SecondsSinceLastJournalWrite { get; set; }

        // null when the platform gives no queue depth signal (Windows, containers without sysfs)
        public double? QueueDepth { get; set; }

        public double SyncCostInMs { get; set; }

        // OS disk counters, the same as the dashboard IO Stats widget; null until the first sample or when the OS gives none
        public StorageDiskStatsInfo DiskStats { get; set; }

        public bool Draining { get; set; }

        public DateTime? LastModeSwitch { get; set; }

        public double DrainShareOfLastHour { get; set; }

        public StorageDeviceThresholds Thresholds { get; set; }

        public List<StorageDeviceSample> History { get; set; }

        // the worst slow write per file on this disk within the last hour, worst first
        public List<SlowIoDetails.SlowWriteInfo> SlowWrites { get; set; }

        public static StorageDeviceIoInfo Create(DeviceWriteBudget device, DeviceTracker tracker, StorageDeviceMonitor monitor)
        {
            CongestionThresholds thresholds = monitor.Thresholds;
            DiskStatsResult diskStats = monitor.GetDiskStats(tracker?.MountPoint);
            DeviceWriteBudget.DeviceClass deviceClass = device.Classify(out DeviceWriteBudget.ClassReason classReason);
            TimeSpan sinceLastWrite = device.TimeSinceLastJournalWrite;
            DeviceTracker.Sample? last = null;
            StorageDeviceStatus status = tracker?.GetStatus(thresholds, out last) ?? StorageDeviceStatus.Ok;

            return new StorageDeviceIoInfo
            {
                DeviceId = device.DeviceId.ToString(),
                PathOnDevice = device.PathOnDevice,
                MountPoint = tracker?.MountPoint,
                Status = status,
                StatusReason = GetStatusReason(tracker, status, last, thresholds),
                Class = deviceClass,
                ClassDescription = StorageDeviceMonitor.Describe(classReason, device),
                JournalWriteLatencyInMs = device.JournalWriteLatency.TotalMilliseconds,
                JournalWriteSizeInBytes = device.JournalWriteSizeInBytes,
                SecondsSinceLastJournalWrite = sinceLastWrite == TimeSpan.MaxValue ? null : sinceLastWrite.TotalSeconds,
                QueueDepth = device.QueueDepth,
                SyncCostInMs = device.SyncCost.TotalMilliseconds,
                DiskStats = diskStats == null ? null : new StorageDiskStatsInfo
                {
                    ReadsPerSec = diskStats.IoReadOperations,
                    WritesPerSec = diskStats.IoWriteOperations,
                    ReadBytesPerSec = diskStats.ReadThroughput.GetValue(SizeUnit.Bytes),
                    WriteBytesPerSec = diskStats.WriteThroughput.GetValue(SizeUnit.Bytes)
                },
                Draining = device.IsDraining,
                LastModeSwitch = device.LastModeSwitch,
                DrainShareOfLastHour = tracker?.DrainShareOfLastHour ?? 0,
                Thresholds = new StorageDeviceThresholds
                {
                    SyncCostDrainInMs = device.SyncCostThreshold.TotalMilliseconds,
                    QueueDepthEnterDrain = device.EnterDrainQueueDepth,
                    QueueDepthExitDrain = device.ExitDrainQueueDepth,
                    CongestionDrainShare = thresholds.DrainShare,
                    CongestionJournalWriteLatencyInMs = thresholds.JournalWriteLatencyInMs
                },
                History = tracker?.GetHistory().Select(s => new StorageDeviceSample
                {
                    At = s.At,
                    Draining = s.Draining,
                    JournalWriteLatencyInMs = s.JournalWriteLatencyInMs,
                    QueueDepth = s.QueueDepth
                }).ToList() ?? new List<StorageDeviceSample>(),
                SlowWrites = tracker?.GetSlowWrites(DateTime.UtcNow) ?? new List<SlowIoDetails.SlowWriteInfo>()
            };
        }

        internal static string GetStatusReason(DeviceTracker tracker, StorageDeviceStatus status, DeviceTracker.Sample? last, CongestionThresholds thresholds)
        {
            switch (status)
            {
                case StorageDeviceStatus.Saturated:
                    return $"Saturated since {tracker.SaturatedSince:u}: {tracker.SaturationCause}.";
                case StorageDeviceStatus.Busy:
                    return last.Value.Draining
                        ? "Writes were paced in the last 10 seconds: the disk is congested."
                        : $"Journal writes took {last.Value.JournalWriteLatencyInMs:#,#0.#} ms in the last 10 seconds (threshold {thresholds.JournalWriteLatencyInMs:#,#0} ms).";
                default:
                    return tracker == null ? "No data yet." : "The disk keeps up with the writes.";
            }
        }
    }

    public sealed class StorageDiskStatsInfo
    {
        public double ReadsPerSec { get; set; }

        public double WritesPerSec { get; set; }

        public long ReadBytesPerSec { get; set; }

        public long WriteBytesPerSec { get; set; }
    }

    public sealed class StorageDeviceThresholds
    {
        public double SyncCostDrainInMs { get; set; }

        public double QueueDepthEnterDrain { get; set; }

        public double QueueDepthExitDrain { get; set; }

        public double CongestionDrainShare { get; set; }

        public double CongestionJournalWriteLatencyInMs { get; set; }
    }

    public sealed class StorageDeviceSample
    {
        public DateTime At { get; set; }

        public bool Draining { get; set; }

        // null = no journal write in that interval
        public double? JournalWriteLatencyInMs { get; set; }

        public double? QueueDepth { get; set; }
    }

    public sealed class StorageEnvironmentIoInfo
    {
        // null for the server's own (System) environment
        public string Database { get; set; }

        public StorageEnvironmentWithType.StorageEnvironmentType Type { get; set; }

        public string Name { get; set; }

        // null when the device could not be identified, the environment then uses its own write budget
        public string DeviceId { get; set; }

        public StorageEnvironmentCompressionInfo Compression { get; set; }

        public StorageEnvironmentPipeliningInfo Pipelining { get; set; }

        public bool ConsolidatingBatches { get; set; }

        public long FlusherYields { get; set; }

        public StorageEnvironmentSyncInfo Sync { get; set; }

        public static StorageEnvironmentIoInfo Create(StorageDeviceMonitor.EnvironmentOfDatabase env)
        {
            StorageEnvironment environment = env.Environment.Environment;
            StorageEnvironmentOptions options = environment.Options;
            WriteFlowPolicy flow = environment.WriteFlow;
            WriteFlowPolicy.SyncInfo lastSync = flow.LastSync;

            return new StorageEnvironmentIoInfo
            {
                Database = env.Database,
                Type = env.Environment.Type,
                Name = env.Environment.Name,
                DeviceId = options.DeviceWriteBudget?.DeviceId.ToString(),
                Compression = new StorageEnvironmentCompressionInfo
                {
                    Configured = options.JournalCompressionAlgorithm,
                    Resolved = flow.ResolveJournalCompressionAlgorithm(options.JournalCompressionAlgorithm),
                    CompressAboveInBytes = flow.GetCompressTxAboveSizeInBytes(options.CompressTxAboveSizeInBytes)
                },
                Pipelining = new StorageEnvironmentPipeliningInfo
                {
                    Active = flow.IsPipelining,
                    JournalWriteLatencyInMs = flow.JournalWriteLatency.TotalMilliseconds,
                    JournalWriteSizeInBytes = flow.JournalWriteSizeInBytes
                },
                ConsolidatingBatches = flow.ConsolidatingBatches,
                FlusherYields = flow.FlusherYields,
                Sync = new StorageEnvironmentSyncInfo
                {
                    WrittenButUnsyncedInBytes = environment.Journal.Applicator.TotalWrittenButUnsyncedBytes,
                    SoftLimitInBytes = options.MaxUnsyncedBytesBeforeSync,
                    MandatoryLimitInBytes = options.MaxUnsyncedBytesBeforeMandatorySync,
                    LastReason = lastSync?.Reason,
                    LastSizeInBytes = lastSync?.Bytes,
                    LastTotalInMs = lastSync?.Total.TotalMilliseconds,
                    LastWritebackInMs = lastSync?.Writeback.TotalMilliseconds,
                    LastDrain = lastSync?.Drain,
                    LastAt = lastSync?.At
                }
            };
        }
    }

    public sealed class StorageEnvironmentCompressionInfo
    {
        public JournalCompressionAlgorithm Configured { get; set; }

        public JournalCompressionAlgorithm Resolved { get; set; }

        public long CompressAboveInBytes { get; set; }
    }

    public sealed class StorageEnvironmentPipeliningInfo
    {
        public bool Active { get; set; }

        public double JournalWriteLatencyInMs { get; set; }

        public long JournalWriteSizeInBytes { get; set; }
    }

    public sealed class StorageEnvironmentSyncInfo
    {
        public long WrittenButUnsyncedInBytes { get; set; }

        public long SoftLimitInBytes { get; set; }

        public long MandatoryLimitInBytes { get; set; }

        public IoMetrics.SyncReason? LastReason { get; set; }

        public long? LastSizeInBytes { get; set; }

        public double? LastTotalInMs { get; set; }

        public double? LastWritebackInMs { get; set; }

        public bool? LastDrain { get; set; }

        public DateTime? LastAt { get; set; }
    }
}
