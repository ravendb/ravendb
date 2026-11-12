using System.IO;
using System.IO.Compression;
using System.Text.Json;
using System.Text.Json.Nodes;
using FastTests;
using Raven.Server.Documents.Handlers.Debugging.DebugPackage;
using Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers;
using Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Errors;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_27704 : NoDisposalNeeded
{
    private const string ClusterTopologyEntry = "server-wide/cluster.topology.json";

    public RavenDB_27704(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Debug)]
    public void CanGetSummaryOfDebugPackageWithoutNodeLicenseDetails()
    {
        // an unlicensed node has no license details (so no OS info) in its cluster topology until UpdateLicenseLimitsCommand runs
        using var debugPackageStream = GetPackageWithoutNodeLicenseDetails("SlowTests.Data.RavenDB_14548.debug-package - Node [A].zip");

        var packageReport = new DebugPackageAnalyzer(debugPackageStream).Analyze();

        var report = packageReport.ForNode("A");
        Assert.Null(report.Machine.OsInfo);
        Assert.Contains(report.AnalyzeErrors.Errors, x => x.ComponentName == nameof(MachineInfoAnalyzer) && x.Severity == AnalyzeErrorSeverity.Warning);

        var summary = packageReport.GetSummary();
        Assert.NotNull(summary.ToJson());

        var nodeSummary = summary.SummaryPerNode["A"];
        Assert.Equal("A", nodeSummary.ClusterNodeInfo.NodeTag);
        Assert.Null(nodeSummary.ClusterNodeInfo.OsName);
        Assert.Equal("7.0.2-nightly-20250415-0350", nodeSummary.ClusterNodeInfo.ServerVersion);
    }

    private static MemoryStream GetPackageWithoutNodeLicenseDetails(string packageResourceName)
    {
        var result = new MemoryStream();

        using (var source = new ZipArchive(typeof(RavenDB_27704).Assembly.GetManifestResourceStream(packageResourceName), ZipArchiveMode.Read))
        using (var target = new ZipArchive(result, ZipArchiveMode.Create, leaveOpen: true))
        {
            foreach (var entry in source.Entries)
            {
                using var entryStream = entry.Open();
                using var targetStream = target.CreateEntry(entry.FullName).Open();

                if (entry.FullName != ClusterTopologyEntry)
                {
                    entryStream.CopyTo(targetStream);
                    continue;
                }

                var topology = JsonNode.Parse(entryStream).AsObject();
                Assert.True(topology.Remove("NodeLicenseDetails"));

                using var writer = new Utf8JsonWriter(targetStream);
                topology.WriteTo(writer);
            }
        }

        result.Position = 0;
        return result;
    }
}
