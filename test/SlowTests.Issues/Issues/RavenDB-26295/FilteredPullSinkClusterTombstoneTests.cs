using System;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.Replication;
using SlowTests.Issues.RavenDB_26295.Tools;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues.RavenDB_26295;

public class FilteredPullSinkClusterTombstoneTests : FilteredPullDualClusterTestBase
{
    public FilteredPullSinkClusterTombstoneTests(ITestOutputHelper output) : base(output)
    {
    }

    private const int TicketCount = 5;

    [RavenFact(RavenTestCategory.Replication | RavenTestCategory.Cluster | RavenTestCategory.Certificates)]
    public async Task SinkClusterTombstones_ShouldStayIdentical_WhenSinkToHubChannelIsBroken()
    {
        // 1 hub node, 3 sink nodes. Sink->hub filtered replication pinned to sink node A.
        await using var lab = await CreateDualClusterLabAsync(new Options(), hubNodeCount: 1, sinkNodeCount: 3);
        await ConfigureSinkToHubReplicationAsync(lab, sinkOwner: LabNode.A, hubNode: LabNode.A);

        var sinkNodes = NodesOnEachClusterSide; // A, B, C

        // Store tickets on the sink owner and confirm the sink->hub channel works (docs reach the hub and every sink node).
        for (var i = 0; i < TicketCount; i++)
            await lab.StoreTicketAsync(ClusterSide.Sink, LabNode.A, $"tickets/{i}", $"ticket-{i}");

        for (var i = 0; i < TicketCount; i++)
        {
            await lab.WaitForTicketAsync(ClusterSide.Hub, LabNode.A, $"tickets/{i}", $"ticket-{i}");
            foreach (var node in sinkNodes)
                await lab.WaitForTicketAsync(ClusterSide.Sink, node, $"tickets/{i}", $"ticket-{i}");
        }

        // Break the sink->hub channel by freezing the owning sink node's replication.
        using var sinkToHubBreak = await BreakReplication(lab.GetServer(ClusterSide.Sink, LabNode.A).ServerStore, lab.SinkDatabaseName);

        // Delete the documents on a different sink node so the tombstones spread cluster-wide through internal replication.
        for (var i = 0; i < TicketCount; i++)
            await lab.DeleteDocumentAsync(ClusterSide.Sink, LabNode.B, $"tickets/{i}");

        // Wait until each sink node observes all tombstones (internal replication completed and was confirmed).
        foreach (var node in sinkNodes)
        {
            var reached = await WaitForValueAsync(() => lab.GetNumberOfDocumentTombstones(ClusterSide.Sink, node), TicketCount, timeout: 30_000, interval: 100);
            Assert.Equal(TicketCount, (int)reached);
        }

        // ---- Phase 1: channel broken -> every sink node retains the SAME, non-zero set ----
        // The cleaner must not delete tombstones the hub has not confirmed yet - and that must hold on every node,
        // not only the one that personally runs the outgoing sink->hub handler.
        foreach (var node in sinkNodes)
            await lab.RunTombstoneCleanupAsync(ClusterSide.Sink, node);

        foreach (var node in sinkNodes)
        {
            var count = lab.GetNumberOfDocumentTombstones(ClusterSide.Sink, node);
            Assert.True(
                count == TicketCount,
                $"While the sink->hub channel is broken every sink node must retain all {TicketCount} tombstones, " +
                $"but {NodeTag(ClusterSide.Sink, node)} has {count}. Per-node: {SinkTombstoneReport(lab)}.");
        }

        // ---- Phase 2: mend the channel and let replication move forward -> tombstones get cleaned everywhere ----
        await sinkToHubBreak.MendAsync();

        // Author a fresh ticket on every sink node and confirm it reaches the hub and all sinks. This advances each
        // node's component in the sink cursor past the tombstone etags, which is what lets the cleaner release them.
        foreach (var node in sinkNodes)
            await lab.StoreTicketAsync(ClusterSide.Sink, node, $"tickets/after-mend-{node}", $"after-mend-{node}");

        foreach (var authored in sinkNodes)
        {
            await lab.WaitForTicketAsync(ClusterSide.Hub, LabNode.A, $"tickets/after-mend-{authored}", $"after-mend-{authored}");
            foreach (var node in sinkNodes)
                await lab.WaitForTicketAsync(ClusterSide.Sink, node, $"tickets/after-mend-{authored}", $"after-mend-{authored}");
        }

        // Every sink node must now release the tombstones (the originals only - the fresh docs are live).
        foreach (var node in sinkNodes)
        {
            long remaining = -1;
            for (var attempt = 0; attempt < 30 && remaining != 0; attempt++)
            {
                await lab.RunTombstoneCleanupAsync(ClusterSide.Sink, node);
                remaining = lab.GetNumberOfDocumentTombstones(ClusterSide.Sink, node);
                if (remaining != 0)
                    await Task.Delay(200);
            }

            Assert.True(
                remaining == 0,
                $"Once replication moved forward, {NodeTag(ClusterSide.Sink, node)} should clean its tombstones but still has {remaining}. " +
                $"Per-node: {SinkTombstoneReport(lab)}.");
        }
    }

    private static string SinkTombstoneReport(DualClusterLab lab) =>
        string.Join(", ", NodesOnEachClusterSide.Select(node => $"{NodeTag(ClusterSide.Sink, node)}={lab.GetNumberOfDocumentTombstones(ClusterSide.Sink, node)}"));

    // Scenario-specific wiring: a sink->hub-only filtered pull task pinned to one sink node, built from the lab's
    // general connection inputs (stores, hub database, hub URLs, pull certificate).
    private async Task ConfigureSinkToHubReplicationAsync(DualClusterLab lab, LabNode sinkOwner, LabNode hubNode)
    {
        var sinkStore = lab.GetStore(ClusterSide.Sink, sinkOwner);
        var connectionStringName = $"sink-to-hub-{Guid.NewGuid():N}";

        await sinkStore.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(new RavenConnectionString
        {
            Name = connectionStringName,
            Database = lab.HubDatabaseName,
            TopologyDiscoveryUrls = [lab.GetServer(ClusterSide.Hub, hubNode).WebUrl]
        }));

        await sinkStore.Maintenance.SendAsync(new UpdatePullReplicationAsSinkOperation(new PullReplicationAsSink
        {
            Name = connectionStringName,
            ConnectionStringName = connectionStringName,
            Mode = PullReplicationMode.SinkToHub,
            HubName = HubName(hubNode),
            AccessName = HubAccessName,
            CertificateWithPrivateKey = lab.PullCertificate,
            PinToMentorNode = true,
            MentorNode = NodeTag(ClusterSide.Sink, sinkOwner),
            AllowedSinkToHubPaths = ["tickets/*"]
        }));
    }
}
