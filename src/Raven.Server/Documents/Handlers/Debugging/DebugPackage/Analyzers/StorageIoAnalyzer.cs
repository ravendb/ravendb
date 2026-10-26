using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Errors;
using Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Issues;
using Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Results;
using Raven.Server.Storage;
using Raven.Server.Web.System;
using Voron.Impl.Journal;

namespace Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers;

/// <summary>
/// Is the disk holding this node back? Reads /admin/debug/storage/io when the package has it, otherwise derives the device state from
/// the io-metrics (journal write latency and size, data file sync cost) the way the server's own DeviceWriteBudget would.
/// </summary>
public class StorageIoAnalyzer(DebugPackageContent packageContent, DebugPackageAnalyzeErrors errors, DebugPackageAnalysisIssues issues)
    : AbstractDebugPackageAnalyzer(errors, issues)
{
    // defaults of PerformanceHints.Storage.CongestionJournalWriteLatencyInMs, Storage.SyncWritebackBarrierCostThresholdInMs,
    // Storage.PipelineJournalWritesAboveLatencyInTicks - used when the package does not carry the thresholds
    internal const double DefaultCongestionLatencyInMs = 50;
    internal const double DefaultSyncCostDrainInMs = 100;
    internal const double DefaultClassifyAboveLatencyInMs = 2;
    internal const double DefaultCongestionDrainShare = 0.5;

    private const string RecommendedAction =
        "Consider a volume with more IOPS / throughput, an instance with higher storage bandwidth, or more RAM to reduce disk reads. " +
        "Live view: Manage Server > Debug > Advanced > Storage I/O.";

    public StorageIoAnalysisInfo StorageIo { get; private set; }

    protected override bool RetrieveAnalyzerInfo(DebugPackageEntries serverEntries)
    {
        StorageIo = new StorageIoAnalysisInfo();

        var databaseMetrics = new Dictionary<string, IoMetricsDump>();
        foreach (string database in packageContent.DatabaseNames)
        {
            if (packageContent.ForDatabase(database).TryGetValue<IoMetricsHandler, IoMetricsDump>(x => x.Get(), out IoMetricsDump dump) && dump?.Environments != null)
                databaseMetrics[database] = dump;
        }

        foreach (var (database, dump) in databaseMetrics)
            StorageIo.Databases.Add(SummarizeDatabase(database, dump));

        if (serverEntries.TryGetValue<AdminStorageIoHandler, StorageIoReport>(x => x.StorageIo(), out StorageIoReport report) && report?.Devices != null)
        {
            StorageIo.FromStorageIoEndpoint = true;
            StorageIo.SharedJournals = report.SharedJournals ?? new List<SharedJournalsIoInfo>();
            foreach (StorageDeviceIoInfo device in report.Devices)
                StorageIo.Devices.Add(FromEndpoint(device));
        }
        else
        {
            serverEntries.TryGetValue<AdminIoMetricsHandler, IoMetricsDump>(x => x.IoMetrics(), out IoMetricsDump systemMetrics);
            var all = databaseMetrics.Values.Append(systemMetrics).Where(x => x?.Environments != null).ToList();
            if (all.Count == 0)
            {
                AddWarning("No io-metrics in the package, cannot tell whether the disk holds this node back");
                return false;
            }

            StorageIo.Devices.Add(Derive(all));
        }

        StorageIo.Worst = StorageIo.Devices
            .OrderByDescending(d => d.IsBottleneck)
            .ThenByDescending(d => d.JournalWriteLatencyInMs)
            .FirstOrDefault();

        return true;
    }

    protected override void DetectIssues(DebugPackageAnalysisIssues issues)
    {
        foreach (StorageIoDeviceAnalysis device in StorageIo.Devices)
        {
            if (device.IsBottleneck)
            {
                string where = StorageIo.FromStorageIoEndpoint ? $" ({device.Name})" : string.Empty;
                issues.ServerIssues.Add(new DetectedIssue(
                    device.Status == StorageDeviceStatus.Saturated ? $"Storage is saturated{where}" : $"The disk is the bottleneck of this node{where}",
                    Describe(device),
                    IssueSeverity.Warning, IssueCategory.Server)
                {
                    RecommendedAction = RecommendedAction
                });
            }

            // a congested disk taking tiny writes is never classified, so Auto compression stays on LZ4 exactly where Zstd would help
            if (device.IsBottleneck && device.Class == DeviceWriteBudget.DeviceClass.Unknown && device.JournalWriteSizeInBytes > 0 &&
                device.JournalWriteSizeInBytes < DeviceWriteBudget.SmallWriteBytes && device.JournalWriteLatencyInMs >= DefaultClassifyAboveLatencyInMs / 2)
            {
                issues.ServerIssues.Add(new DetectedIssue(
                    "Slow disk not detected as bandwidth limited",
                    $"Journal writes average {device.JournalWriteSizeInBytes / 1024.0:#,#0.#} KB, below the {DeviceWriteBudget.SmallWriteBytes / 1024:#,#0} KB needed to classify the disk, " +
                    $"while taking {device.JournalWriteLatencyInMs:#,#0.#} ms. The adaptive write path keeps its safe defaults: Auto journal compression stays on LZ4 " +
                    "although this disk is the bottleneck.",
                    IssueSeverity.Info, IssueCategory.Server)
                {
                    RecommendedAction = "If the disk is a metered cloud volume, consider setting Storage.JournalsCompressionAlgorithm to Zstd explicitly."
                });
            }
        }

        foreach (StorageIoDatabaseAnalysis database in StorageIo.Databases)
        {
            if (database.MandatorySyncShare is > 0.5 && database.Syncs >= 4)
            {
                issues.ForDatabase(database.Database).Add(new DetectedIssue(
                    "Most data file syncs are mandatory",
                    $"{database.MandatorySyncShare:P0} of {database.Syncs:#,#} syncs ran because the unsynced data passed the mandatory limit " +
                    $"(Storage.MaxUnsyncedSizeBeforeMandatorySyncInMb), average {database.SyncAverageInMs:#,#0} ms, max {database.SyncMaxInMs:#,#0} ms. " +
                    "The disk cannot keep up with the writes between syncs.",
                    IssueSeverity.Info, IssueCategory.Database));
            }
        }

        foreach (SharedJournalsIoInfo shared in StorageIo.SharedJournals)
        {
            var fellBack = shared.IndexesNotSharing?.Where(x => x.Reason == IndexNotSharingReason.HardLinkLimit).ToList() ?? new List<IndexNotSharingInfo>();
            if (fellBack.Count > 0)
            {
                issues.ForDatabase(shared.Database).Add(new DetectedIssue(
                    "Indexes write their own journals",
                    $"{fellBack.Count:#,#} index(es) left the shared index journal because the file system hard link limit was reached: " +
                    string.Join(", ", fellBack.Select(x => $"'{x.Index}' (since {x.Since:u})")) + ". They now pay for their own journal writes and disk space.",
                    IssueSeverity.Warning, IssueCategory.Indexes)
                {
                    RecommendedAction = "The indexes rejoin the shared journal when the database restarts."
                });
            }

            if (shared.MergedCommitsQueue >= 64 || shared.CommitToDurableInMs >= 1000)
            {
                issues.ForDatabase(shared.Database).Add(new DetectedIssue(
                    "Index commits wait for the shared index journal",
                    $"{shared.MergedCommitsQueue:#,#0} index commit(s) are queued for the shared index journal and a commit takes {shared.CommitToDurableInMs:#,#0} ms to become durable.",
                    IssueSeverity.Warning, IssueCategory.Indexes));
            }
        }
    }

    // a node whose disk is the bottleneck while its peers are fine is the most useful thing to say about a cluster
    public static void DetectClusterWideIssues(List<DebugPackageNodeReport> reports, List<DetectedIssue> clusterWideIssues)
    {
        var nodes = reports.Where(r => r.Server?.StorageIo?.Worst != null).ToList();
        if (nodes.Count < 2)
            return;

        var healthy = nodes.Where(r => r.Server.StorageIo.Worst.IsBottleneck == false).ToList();
        if (healthy.Count == 0)
            return;

        foreach (DebugPackageNodeReport slow in nodes.Where(r => r.Server.StorageIo.Worst.IsBottleneck))
        {
            StorageIoDeviceAnalysis worst = slow.Server.StorageIo.Worst;
            string peers = string.Join(" and ", healthy.Select(r => $"{r.Server.StorageIo.Worst.JournalWriteLatencyInMs:#,#0.#} ms on {r.NodeTag}"));

            clusterWideIssues.Add(new DetectedIssue(
                $"Node {slow.NodeTag} is held back by its disk",
                $"Journal writes take {worst.JournalWriteLatencyInMs:#,#0.#} ms on {slow.NodeTag} vs {peers}. {Describe(worst, includeLatency: false)}",
                IssueSeverity.Warning, IssueCategory.Cluster)
            {
                RecommendedAction = RecommendedAction
            });
        }
    }

    private static string Describe(StorageIoDeviceAnalysis device, bool includeLatency = true)
    {
        if (device.StatusReason != null)
            return device.StatusReason;

        var sentences = new List<string>();
        if (includeLatency)
            sentences.Add($"Journal writes average {device.JournalWriteLatencyInMs:#,#0.#} ms for {device.JournalWriteSizeInBytes / 1024.0:#,#0.#} KB.");
        if (device.LikelyDraining)
            sentences.Add($"Data file syncs average {device.SyncCostInMs:#,#0} ms, above the {DefaultSyncCostDrainInMs:#,#0} ms at which writes get paced (drain mode).");
        if (device.From != null)
            sentences.Add($"Derived from the io-metrics in the package, {device.From:u} - {device.To:u}.");

        return string.Join(" ", sentences);
    }

    private static StorageIoDeviceAnalysis FromEndpoint(StorageDeviceIoInfo device)
    {
        double latencyThreshold = device.Thresholds?.CongestionJournalWriteLatencyInMs ?? DefaultCongestionLatencyInMs;
        return new StorageIoDeviceAnalysis
        {
            Name = device.MountPoint ?? device.PathOnDevice,
            Status = device.Status,
            StatusReason = device.StatusReason,
            Class = device.Class,
            ClassDescription = device.ClassDescription,
            DrainShareOfLastHour = device.DrainShareOfLastHour,
            LikelyDraining = device.Draining,
            JournalWriteLatencyInMs = device.JournalWriteLatencyInMs,
            JournalWriteSizeInBytes = device.JournalWriteSizeInBytes,
            SyncCostInMs = device.SyncCostInMs,
            QueueDepth = device.QueueDepth,
            IsBottleneck = device.Status == StorageDeviceStatus.Saturated ||
                           device.DrainShareOfLastHour > (device.Thresholds?.CongestionDrainShare ?? DefaultCongestionDrainShare) ||
                           (device.SecondsSinceLastJournalWrite < 60 && device.JournalWriteLatencyInMs > latencyThreshold)
        };
    }

    // mirrors DeviceWriteBudget: EWMAs seeded by the first sample, smoothing 8 for journal writes and 4 for syncs
    internal static StorageIoDeviceAnalysis Derive(List<IoMetricsDump> dumps)
    {
        var writes = new List<IoMetricsItem>();
        var syncs = new List<IoMetricsItem>();
        foreach (IoMetricsItem item in dumps.SelectMany(Items))
        {
            if (item.Type == "JournalWrite")
                writes.Add(item);
            else if (item.Type == "DataSync")
                syncs.Add(item);
        }

        writes.Sort((x, y) => x.StartTime.CompareTo(y.StartTime));
        syncs.Sort((x, y) => x.StartTime.CompareTo(y.StartTime));

        double latency = Ewma(writes.Select(w => w.Duration), 8);
        double size = Ewma(writes.Select(w => (double)w.Size), 8);
        double syncCost = Ewma(syncs.Select(s => s.Duration), 4);

        var device = new StorageIoDeviceAnalysis
        {
            Name = "All disks (from io-metrics)",
            JournalWriteLatencyInMs = latency,
            JournalWriteSizeInBytes = (long)size,
            SyncCostInMs = syncCost,
            LikelyDraining = syncCost > DefaultSyncCostDrainInMs,
            JournalWrites = writes.Count,
            Syncs = syncs.Count,
            From = writes.Count > 0 ? writes[0].StartTime : null,
            To = writes.Count > 0 ? writes[^1].StartTime : null
        };

        (device.Class, device.ClassDescription) = Classify(latency, size);
        device.IsBottleneck = device.LikelyDraining || latency > DefaultCongestionLatencyInMs;
        return device;
    }

    private static (DeviceWriteBudget.DeviceClass, string) Classify(double latencyInMs, double sizeInBytes)
    {
        double threshold = DefaultClassifyAboveLatencyInMs;
        if (latencyInMs == 0)
            return (DeviceWriteBudget.DeviceClass.Unknown, "no journal writes in the package");
        if (latencyInMs < threshold / 8)
            return (DeviceWriteBudget.DeviceClass.Fast, $"journal writes take {latencyInMs:#,#0.##} ms, below 1/8 of {threshold} ms");
        if (sizeInBytes < DeviceWriteBudget.SmallWriteBytes)
            return (DeviceWriteBudget.DeviceClass.Unknown,
                $"journal writes average {sizeInBytes / 1024:#,#0} KB, below {DeviceWriteBudget.SmallWriteBytes / 1024:#,#0} KB, too small to tell a slow disk from a fast one ({latencyInMs:#,#0.##} ms)");
        return latencyInMs < threshold / 2
            ? (DeviceWriteBudget.DeviceClass.Fast, $"journal writes take {latencyInMs:#,#0.##} ms, below half of {threshold} ms")
            : (DeviceWriteBudget.DeviceClass.Budgeted, $"journal writes take {latencyInMs:#,#0.##} ms, at or above half of {threshold} ms");
    }

    private static StorageIoDatabaseAnalysis SummarizeDatabase(string database, IoMetricsDump dump)
    {
        var items = Items(dump).ToList();
        var writes = items.Where(x => x.Type == "JournalWrite").ToList();
        var syncs = items.Where(x => x.Type == "DataSync").ToList();
        var syncsWithReason = syncs.Where(x => x.SyncReason != null).ToList();
        var writesWithFlag = writes.Where(x => x.Pipelined != null).ToList();
        double totalSync = syncs.Sum(x => x.Duration);

        return new StorageIoDatabaseAnalysis
        {
            Database = database,
            CompressionAlgorithms = items.Where(x => x.Type == "Compression" && x.Algorithm != null).Select(x => x.Algorithm).Distinct().OrderBy(x => x).ToList(),
            JournalWrites = writes.Count,
            JournalWriteAverageInMs = writes.Count > 0 ? writes.Average(x => x.Duration) : 0,
            PipelinedShare = writesWithFlag.Count > 0 ? writesWithFlag.Count(x => x.Pipelined == true) / (double)writesWithFlag.Count : null,
            Syncs = syncs.Count,
            SyncAverageInMs = syncs.Count > 0 ? syncs.Average(x => x.Duration) : 0,
            SyncMaxInMs = syncs.Count > 0 ? syncs.Max(x => x.Duration) : 0,
            MandatorySyncShare = syncsWithReason.Count > 0 ? syncsWithReason.Count(x => x.SyncReason == "Mandatory") / (double)syncsWithReason.Count : null,
            DrainSyncShare = syncsWithReason.Count > 0 ? syncsWithReason.Count(x => x.Drain == true) / (double)syncsWithReason.Count : null,
            WritebackShareOfSyncTime = syncsWithReason.Count > 0 && totalSync > 0 ? syncs.Sum(x => x.WritebackDuration ?? 0) / totalSync : null
        };
    }

    private static IEnumerable<IoMetricsItem> Items(IoMetricsDump dump) =>
        dump.Environments.SelectMany(e => e.Files ?? new List<IoMetricsFile>()).SelectMany(f => f.Recent ?? new List<IoMetricsItem>());

    private static double Ewma(IEnumerable<double> values, int smoothing)
    {
        double current = 0;
        bool seeded = false;
        foreach (double value in values)
        {
            current = seeded ? current + (value - current) / smoothing : value;
            seeded = true;
        }

        return current;
    }

    // the shape of /admin/debug/io-metrics and /databases/*/debug/io-metrics, only what this analyzer reads
    public class IoMetricsDump
    {
        public List<IoMetricsEnvironment> Environments { get; set; }
    }

    public class IoMetricsEnvironment
    {
        public string Path { get; set; }

        public string Type { get; set; }

        public List<IoMetricsFile> Files { get; set; }
    }

    public class IoMetricsFile
    {
        public string File { get; set; }

        public List<IoMetricsItem> Recent { get; set; }
    }

    public class IoMetricsItem
    {
        public string Start { get; set; }

        public double Duration { get; set; }

        public long Size { get; set; }

        public string Type { get; set; }

        public string Algorithm { get; set; }

        public bool? Pipelined { get; set; }

        public string SyncReason { get; set; }

        public bool? Drain { get; set; }

        public double? WritebackDuration { get; set; }

        public DateTime StartTime => DateTime.TryParse(Start, CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal, out DateTime start)
            ? start
            : DateTime.MinValue;
    }
}
