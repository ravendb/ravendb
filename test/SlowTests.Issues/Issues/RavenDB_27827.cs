using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text.Json;
using FastTests;
using Raven.Server.Documents.Handlers.Debugging.DebugPackage;
using Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Issues;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.Storage;
using Sparrow.Server.Platform;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_27827 : NoDisposalNeeded
{
    public RavenDB_27827(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Core)]
    public void Analyzer_flags_the_node_whose_disk_holds_it_back_from_io_metrics_of_an_older_package()
    {
        // one slow node (journal writes ~97ms, 1.2s syncs), the others under 1ms
        using var package = ClusterPackage(new Dictionary<string, Dictionary<string, object>>
        {
            ["A"] = new() { ["server-wide/admin.io-metrics.json"] = IoMetrics(journalWriteMs: 0.9, syncMs: 8) },
            ["B"] = new() { ["server-wide/admin.io-metrics.json"] = IoMetrics(journalWriteMs: 97, syncMs: 1200) },
            ["C"] = new() { ["server-wide/admin.io-metrics.json"] = IoMetrics(journalWriteMs: 0.9, syncMs: 1) }
        });

        var report = new DebugPackageAnalyzer(package).Analyze();

        var held = Assert.Single(report.ClusterWideIssues.ClusterIssues, x => x.Title.Contains("held back by its disk"));
        Assert.Equal("Node B is held back by its disk", held.Title);
        Assert.Equal(IssueSeverity.Warning, held.Severity);

        Assert.Contains(report.ForNode("B").DetectedIssues.ServerIssues, x => x.Title == "The disk is the bottleneck of this node");
        Assert.DoesNotContain(report.ForNode("A").DetectedIssues.ServerIssues, x => x.Title.Contains("bottleneck"));
        Assert.DoesNotContain(report.ForNode("C").DetectedIssues.ServerIssues, x => x.Title.Contains("bottleneck"));

        var b = report.ForNode("B").Server.StorageIo;
        Assert.False(b.FromStorageIoEndpoint);
        Assert.True(b.Worst.LikelyDraining);
    }

    [RavenFact(RavenTestCategory.Core)]
    public void Analyzer_reads_the_storage_io_state_when_the_package_has_it()
    {
        var saturated = new StorageIoReport
        {
            Devices =
            [
                new StorageDeviceIoInfo
                {
                    DeviceId = "2049", MountPoint = "/ravendb", PathOnDevice = "/ravendb/data/Databases/Orders/Raven.voron",
                    Status = StorageDeviceStatus.Saturated,
                    StatusReason = "Saturated since 2026-10-26 06:00:00Z: writes were paced more than 50% of an hour because the disk was congested.",
                    DrainShareOfLastHour = 0.93, JournalWriteLatencyInMs = 97, JournalWriteSizeInBytes = 7 * 1024, SyncCostInMs = 1200
                }
            ],
            Environments = [],
            SharedJournals =
            [
                new SharedJournalsIoInfo
                {
                    Database = "Orders", Files = [], IndexesSharing = 3,
                    IndexesNotSharing = [new IndexNotSharingInfo { Index = "Orders/Totals", Reason = IndexNotSharingReason.HardLinkLimit, Since = DateTime.UtcNow }]
                }
            ]
        };

        using var package = ClusterPackage(new Dictionary<string, Dictionary<string, object>>
        {
            ["A"] = new() { ["server-wide/admin.storage.io.json"] = new StorageIoReport { Devices = [], Environments = [], SharedJournals = [] } },
            ["B"] = new() { ["server-wide/admin.storage.io.json"] = saturated }
        });

        var report = new DebugPackageAnalyzer(package).Analyze();
        var b = report.ForNode("B");

        Assert.True(b.Server.StorageIo.FromStorageIoEndpoint);
        var issue = Assert.Single(b.DetectedIssues.ServerIssues, x => x.Title.StartsWith("Storage is saturated"));
        Assert.Equal("Storage is saturated (/ravendb)", issue.Title);
        Assert.Contains(b.DetectedIssues.ForDatabase("Orders"), x => x.Title == "Indexes write their own journals" && x.Description.Contains("Orders/Totals"));
    }

    [RavenFact(RavenTestCategory.Core)]
    public void Analyzer_flags_a_node_that_writes_data_files_in_a_different_mode()
    {
        using var package = ClusterPackage(new Dictionary<string, Dictionary<string, object>>
        {
            ["A"] = new() { ["server-wide/admin.storage.io.json"] = WithWriteMode(Pal.RvnWriteMode.IoRing) },
            ["B"] = new() { ["server-wide/admin.storage.io.json"] = WithWriteMode(Pal.RvnWriteMode.VectoredFileIo) }
        });

        var report = new DebugPackageAnalyzer(package).Analyze();

        var issue = Assert.Single(report.ClusterWideIssues.ClusterIssues, x => x.Title == "Nodes write data files in different modes");
        Assert.Contains("IoRing on A; VectoredFileIo on B (configured Auto)", issue.Description);
        Assert.Equal(Pal.RvnWriteMode.VectoredFileIo, report.ForNode("B").Server.StorageIo.WriteMode.Effective);
    }

    [RavenFact(RavenTestCategory.Core)]
    public void Analyzer_judges_a_disk_by_the_last_hour_in_the_package_not_by_the_moment_it_was_taken()
    {
        // A: typically 16 ms with a burst and a 9 s write, calm at the moment the package was taken
        var a = Device(StorageDeviceStatus.Saturated, drainShare: 0.9, latencyNow: 3, latencyOfSample: i => i == 150 ? 2000 : 16);
        a.SlowWrites =
        [
            new SlowIoDetails.SlowWriteInfo
            {
                Type = Sparrow.Server.Meters.IoMetrics.MeterType.JournalWrite, Path = "/ravendb/Databases/Orders/Journals/0000000000000000001.journal",
                DataWrittenInMb = 4 / 1024.0, DurationInSec = 9, SpeedInMbPerSec = 4 / 1024.0 / 9, Date = DateTime.UtcNow
            }
        ];

        // B: paced, but its journal writes stay fast
        var b = Device(StorageDeviceStatus.Busy, drainShare: 0.6, latencyNow: 1.9, latencyOfSample: _ => 1.9);
        var c = Device(StorageDeviceStatus.Ok, drainShare: 0, latencyNow: 1, latencyOfSample: _ => 1);

        using var package = ClusterPackage(new Dictionary<string, Dictionary<string, object>>
        {
            ["A"] = new() { ["server-wide/admin.storage.io.json"] = new StorageIoReport { Devices = [a], Environments = [], SharedJournals = [] } },
            ["B"] = new() { ["server-wide/admin.storage.io.json"] = new StorageIoReport { Devices = [b], Environments = [], SharedJournals = [] } },
            ["C"] = new() { ["server-wide/admin.storage.io.json"] = new StorageIoReport { Devices = [c], Environments = [], SharedJournals = [] } }
        });

        var report = new DebugPackageAnalyzer(package).Analyze();

        var saturated = Assert.Single(report.ForNode("A").DetectedIssues.ServerIssues, x => x.Title.StartsWith("Storage is saturated"));
        Assert.Contains("In the last hour journal writes typically took 16 ms (peak 2 s), and the slowest journal write took 9 s for 4 KB.", saturated.Description);

        var heldBack = Assert.Single(report.ClusterWideIssues.ClusterIssues, x => x.Title == "Node A is held back by its disk");
        Assert.StartsWith("Journal writes typically take 16 ms on A vs 1 ms on C.", heldBack.Description);

        // the slow disk rule needs journal writes that are slow over the hour, not a busy disk with fast writes
        Assert.Contains(report.ForNode("A").DetectedIssues.ServerIssues, x => x.Title == "Slow disk not detected as bandwidth limited");
        Assert.DoesNotContain(report.ForNode("B").DetectedIssues.ServerIssues, x => x.Title == "Slow disk not detected as bandwidth limited");
    }

    private static StorageDeviceIoInfo Device(StorageDeviceStatus status, double drainShare, double latencyNow, Func<int, double> latencyOfSample)
    {
        var at = DateTime.UtcNow.AddHours(-1);
        var history = new List<StorageDeviceSample>();
        for (var i = 0; i < 300; i++)
            history.Add(new StorageDeviceSample { At = at = at.AddSeconds(10), Draining = drainShare > 0, JournalWriteLatencyInMs = latencyOfSample(i), QueueDepth = 16 });

        return new StorageDeviceIoInfo
        {
            DeviceId = "2049", MountPoint = "/", PathOnDevice = "/ravendb/System/Raven.voron", Status = status,
            StatusReason = status == StorageDeviceStatus.Saturated ? "Saturated since 2026-10-27 07:30:54Z: writes were paced more than 50% of an hour because the disk was congested." : null,
            DrainShareOfLastHour = drainShare, JournalWriteLatencyInMs = latencyNow, JournalWriteSizeInBytes = 7 * 1024, SecondsSinceLastJournalWrite = 1,
            History = history, SlowWrites = []
        };
    }

    private static StorageIoReport WithWriteMode(Pal.RvnWriteMode effective) => new()
    {
        WriteMode = new StorageWriteModeInfo { Effective = effective, Configured = Pal.RvnWriteMode.Auto },
        Devices = [], Environments = [], SharedJournals = []
    };

    private static object IoMetrics(double journalWriteMs, double syncMs)
    {
        var recent = new List<object>();
        var start = new DateTime(2026, 10, 26, 6, 0, 0, DateTimeKind.Utc);
        for (var i = 0; i < 50; i++)
        {
            recent.Add(new { Start = start.AddSeconds(i).ToString("o"), Duration = journalWriteMs, Size = 8 * 1024, Type = "JournalWrite" });
            if (i % 10 == 0)
                recent.Add(new { Start = start.AddSeconds(i).ToString("o"), Duration = syncMs, Size = 64 * 1024 * 1024, Type = "DataSync" });
        }

        return new { Environments = new[] { new { Path = "/ravendb/data", Type = "System", Files = new[] { new { File = "Raven.voron", Recent = recent } } } } };
    }

    // a cluster wide package: one zip per node, each with its cluster topology (for the node tag) and the given entries
    private static MemoryStream ClusterPackage(Dictionary<string, Dictionary<string, object>> nodes)
    {
        var result = new MemoryStream();
        using (var cluster = new ZipArchive(result, ZipArchiveMode.Create, leaveOpen: true))
        {
            foreach (var (tag, entries) in nodes)
            {
                using var nodeStream = new MemoryStream();
                using (var node = new ZipArchive(nodeStream, ZipArchiveMode.Create, leaveOpen: true))
                {
                    foreach (var (name, content) in entries.Append(new KeyValuePair<string, object>("server-wide/cluster.topology.json", new { NodeTag = tag })))
                    {
                        using var writer = new StreamWriter(node.CreateEntry(name).Open());
                        writer.Write(JsonSerializer.Serialize(content, new JsonSerializerOptions { Converters = { new System.Text.Json.Serialization.JsonStringEnumConverter() } }));
                    }
                }

                using var target = cluster.CreateEntry($"Node - [{tag}].zip").Open();
                nodeStream.Position = 0;
                nodeStream.CopyTo(target);
            }
        }

        result.Position = 0;
        return result;
    }
}
