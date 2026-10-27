using System;
using System.Collections.Generic;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.Storage;
using Voron.Impl.Journal;

namespace Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Results;

public class StorageIoAnalysisInfo
{
    // true: read from /admin/debug/storage/io; false: an older package, derived from the io-metrics it contains
    public bool FromStorageIoEndpoint { get; set; }

    // null in packages without /admin/debug/storage/io
    public StorageWriteModeInfo WriteMode { get; set; }

    public List<StorageIoDeviceAnalysis> Devices { get; set; } = new();

    public List<StorageIoDatabaseAnalysis> Databases { get; set; } = new();

    public List<SharedJournalsIoInfo> SharedJournals { get; set; } = new();

    // the worst device, used to compare nodes
    public StorageIoDeviceAnalysis Worst { get; set; }
}

public class StorageIoDeviceAnalysis
{
    // mount point, or "all disks" when derived from io-metrics (they do not tell which disk a file is on)
    public string Name { get; set; }

    // null when derived from io-metrics
    public StorageDeviceStatus? Status { get; set; }

    public string StatusReason { get; set; }

    public DeviceWriteBudget.DeviceClass Class { get; set; }

    public string ClassDescription { get; set; }

    // null when derived from io-metrics (the queue depth trigger is not in them)
    public double? DrainShareOfLastHour { get; set; }

    // derived: the sync cost is above the drain threshold, so writes were very likely paced
    public bool LikelyDraining { get; set; }

    // the average when the package was taken (endpoint), or over the package's io-metrics
    public double JournalWriteLatencyInMs { get; set; }

    // median and peak of the last hour of 10 s samples; null when derived from io-metrics or no journal write was sampled
    public double? TypicalJournalWriteLatencyInMs { get; set; }

    public double? PeakJournalWriteLatencyInMs { get; set; }

    // null when derived from io-metrics or no slow write in the last hour
    public SlowIoDetails.SlowWriteInfo SlowestWrite { get; set; }

    public long JournalWriteSizeInBytes { get; set; }

    public double SyncCostInMs { get; set; }

    public double? QueueDepth { get; set; }

    public int JournalWrites { get; set; }

    public int Syncs { get; set; }

    public DateTime? From { get; set; }

    public DateTime? To { get; set; }

    public bool IsBottleneck { get; set; }
}

public class StorageIoDatabaseAnalysis
{
    public string Database { get; set; }

    public List<string> CompressionAlgorithms { get; set; } = new();

    public int JournalWrites { get; set; }

    public double JournalWriteAverageInMs { get; set; }

    // null in packages from versions without the field
    public double? PipelinedShare { get; set; }

    public int Syncs { get; set; }

    public double SyncAverageInMs { get; set; }

    public double SyncMaxInMs { get; set; }

    // null in packages from versions without the field
    public double? MandatorySyncShare { get; set; }

    public double? DrainSyncShare { get; set; }

    public double? WritebackShareOfSyncTime { get; set; }
}
