using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text.Json;
using FastTests;
using Raven.Server.Documents.Handlers.Debugging.DebugPackage;
using Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Issues;
using Raven.Server.Storage;
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
                    StatusReason = "Saturated since 2026-10-26 06:00:00Z: writes were paced because the disk was congested 93% of the last hour.",
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
