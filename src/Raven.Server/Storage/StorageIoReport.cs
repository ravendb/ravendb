using System;
using System.Collections.Generic;
using System.Linq;
using Sparrow.Server.Meters;
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
        public List<StorageDeviceIoInfo> Devices { get; set; }

        public List<StorageEnvironmentIoInfo> Environments { get; set; }

        public List<SharedJournalsIoInfo> SharedJournals { get; set; }

        public static StorageIoReport Create(StorageDeviceMonitor monitor)
        {
            CongestionThresholds thresholds = monitor.Thresholds;
            var report = new StorageIoReport
            {
                Devices = new List<StorageDeviceIoInfo>(),
                Environments = new List<StorageEnvironmentIoInfo>(),
                SharedJournals = new List<SharedJournalsIoInfo>()
            };

            var devices = new HashSet<DeviceWriteBudget>(ReferenceEqualityComparer.Instance);
            var environments = monitor.GetEnvironments().ToList();
            foreach (StorageDeviceMonitor.EnvironmentOfDatabase env in environments)
            {
                report.Environments.Add(StorageEnvironmentIoInfo.Create(env));

                DeviceWriteBudget device = env.Environment.Environment.Options.DeviceWriteBudget;
                if (device != null && devices.Add(device))
                    report.Devices.Add(StorageDeviceIoInfo.Create(device, monitor.GetTracker(device), thresholds));
            }

            foreach (var database in environments.Where(e => e.Database != null).GroupBy(e => e.Database))
            {
                SharedJournalsIoInfo sharedJournals = SharedJournalsIoInfo.Create(database.Key, database.ToList());
                if (sharedJournals != null)
                    report.SharedJournals.Add(sharedJournals);
            }

            return report;
        }
    }

    public sealed class StorageDeviceIoInfo
    {
        public string DeviceId { get; set; }

        public string PathOnDevice { get; set; }

        public string MountPoint { get; set; }

        public StorageDeviceStatus Status { get; set; }

        // plain words, also used in the customer facing hint
        public string StatusReason { get; set; }

        public DeviceWriteBudget.DeviceClass Class { get; set; }

        public DeviceWriteBudget.ClassReason ClassReason { get; set; }

        public string ClassDescription { get; set; }

        public double JournalWriteLatencyInMs { get; set; }

        public long JournalWriteSizeInBytes { get; set; }

        // null when no journal write was recorded since the server started
        public double? SecondsSinceLastJournalWrite { get; set; }

        // null when the platform gives no queue depth signal (Windows, containers without sysfs)
        public double? QueueDepth { get; set; }

        public double? QueueDepthAgeInSec { get; set; }

        public double SyncCostInMs { get; set; }

        public bool Draining { get; set; }

        public DateTime? LastModeSwitch { get; set; }

        public double DrainShareOfLastHour { get; set; }

        public DateTime? SaturatedSince { get; set; }

        public StorageDeviceThresholds Thresholds { get; set; }

        public List<StorageDeviceSample> History { get; set; }

        public static StorageDeviceIoInfo Create(DeviceWriteBudget device, DeviceTracker tracker, CongestionThresholds thresholds)
        {
            DeviceWriteBudget.DeviceClass deviceClass = device.Classify(out DeviceWriteBudget.ClassReason classReason);
            TimeSpan sinceLastWrite = device.TimeSinceLastJournalWrite;
            StorageDeviceStatus status = tracker?.GetStatus(thresholds) ?? StorageDeviceStatus.Ok;

            return new StorageDeviceIoInfo
            {
                DeviceId = device.DeviceId.ToString(),
                PathOnDevice = device.PathOnDevice,
                MountPoint = tracker?.MountPoint,
                Status = status,
                StatusReason = GetStatusReason(device, tracker, status, thresholds),
                Class = deviceClass,
                ClassReason = classReason,
                ClassDescription = StorageDeviceMonitor.Describe(classReason, device),
                JournalWriteLatencyInMs = device.JournalWriteLatency.TotalMilliseconds,
                JournalWriteSizeInBytes = device.JournalWriteSizeInBytes,
                SecondsSinceLastJournalWrite = sinceLastWrite == TimeSpan.MaxValue ? null : sinceLastWrite.TotalSeconds,
                QueueDepth = device.HasQueueDepthSignal ? device.QueueDepth : null,
                QueueDepthAgeInSec = device.QueueDepthAge?.TotalSeconds,
                SyncCostInMs = device.SyncCost.TotalMilliseconds,
                Draining = device.IsDraining,
                LastModeSwitch = device.LastModeSwitch,
                DrainShareOfLastHour = tracker?.DrainShareOfLastHour ?? 0,
                SaturatedSince = tracker?.SaturatedSince,
                Thresholds = new StorageDeviceThresholds
                {
                    ClassifyAboveLatencyInMs = device.ClassifyAboveLatency.TotalMilliseconds,
                    SmallWriteInBytes = DeviceWriteBudget.SmallWriteBytes,
                    SyncCostDrainInMs = device.SyncCostThreshold.TotalMilliseconds,
                    QueueDepthEnterDrain = device.EnterDrainQueueDepth,
                    QueueDepthExitDrain = device.ExitDrainQueueDepth,
                    CongestionDrainShare = thresholds.DrainShare,
                    CongestionJournalWriteLatencyInMs = thresholds.JournalWriteLatencyInMs,
                    CongestionSustainedInMin = thresholds.Sustained.TotalMinutes
                },
                History = tracker?.GetHistory().Select(s => new StorageDeviceSample
                {
                    At = s.At,
                    Draining = s.Draining,
                    JournalWriteLatencyInMs = s.JournalWriteLatencyInMs,
                    QueueDepth = s.QueueDepth
                }).ToList() ?? new List<StorageDeviceSample>()
            };
        }

        private static string GetStatusReason(DeviceWriteBudget device, DeviceTracker tracker, StorageDeviceStatus status, CongestionThresholds thresholds)
        {
            switch (status)
            {
                case StorageDeviceStatus.Saturated:
                    return $"Saturated since {tracker.SaturatedSince:u}: {tracker.SaturationCause}.";
                case StorageDeviceStatus.Busy:
                    return device.IsDraining
                        ? "The disk is congested right now, writes are being paced."
                        : $"Journal writes take {device.JournalWriteLatency.TotalMilliseconds:#,#0.#} ms right now (threshold {thresholds.JournalWriteLatencyInMs:#,#0} ms).";
                default:
                    return tracker == null ? "No data yet." : "The disk keeps up with the writes.";
            }
        }
    }

    public sealed class StorageDeviceThresholds
    {
        public double ClassifyAboveLatencyInMs { get; set; }

        public long SmallWriteInBytes { get; set; }

        public double SyncCostDrainInMs { get; set; }

        public double QueueDepthEnterDrain { get; set; }

        public double QueueDepthExitDrain { get; set; }

        public double CongestionDrainShare { get; set; }

        public double CongestionJournalWriteLatencyInMs { get; set; }

        public double CongestionSustainedInMin { get; set; }
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
                    Enabled = flow.IsPipeliningEnabled,
                    Active = flow.IsPipelining,
                    QueueEmptyShare = flow.QueueEmptyShare,
                    BatchOperations = flow.BatchOperations,
                    JournalWriteLatencyInMs = flow.JournalWriteLatency.TotalMilliseconds,
                    JournalWriteSizeInBytes = flow.JournalWriteSizeInBytes,
                    PipelineAboveLatencyInMs = TimeSpan.FromTicks(options.PipelineJournalWritesAboveLatencyInTicks).TotalMilliseconds
                },
                ConsolidatingBatches = flow.ConsolidatingBatches,
                FlusherYields = flow.FlusherYields,
                Sync = new StorageEnvironmentSyncInfo
                {
                    PacedWritebackEnabled = options.SyncWritebackBlockSizeInMb > 0,
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
        public bool Enabled { get; set; }

        public bool Active { get; set; }

        public double QueueEmptyShare { get; set; }

        public double BatchOperations { get; set; }

        public double JournalWriteLatencyInMs { get; set; }

        public long JournalWriteSizeInBytes { get; set; }

        public double PipelineAboveLatencyInMs { get; set; }
    }

    public sealed class StorageEnvironmentSyncInfo
    {
        public bool PacedWritebackEnabled { get; set; }

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
