using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Server;
using Raven.Server.Config;
using Raven.Server.Rachis;
using SlowTests.Issues.RavenDB_26295.Tools;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues.RavenDB_26295;

public abstract class FilteredPullDualClusterTestBase : ReplicationTestBase
{
    protected internal const string HubAccessName = "FilteredPullDualClusterAccess";
    protected internal const string FilteredRoundTripHubName = "Hub-FilteredRoundTrip";

    protected FilteredPullDualClusterTestBase(ITestOutputHelper output) : base(output)
    {
    }

    public enum ClusterSide
    {
        Hub,
        Sink
    }

    public enum LabNode
    {
        A,
        B,
        C
    }

    public sealed class Ticket
    {
        public string Name { get; set; }
    }

    protected internal static readonly LabNode[] NodesOnEachClusterSide = [LabNode.A, LabNode.B, LabNode.C];

    public sealed class TaggedCluster
    {
        public TaggedCluster(List<RavenServer> nodes, RavenServer leader, TestCertificatesHolder certificates)
        {
            Nodes = nodes;
            Leader = leader;
            Certificates = certificates;
        }

        public List<RavenServer> Nodes { get; }

        public RavenServer Leader { get; }

        public TestCertificatesHolder Certificates { get; }
    }

    public sealed class DocumentSnapshot
    {
        public bool Exists { get; init; }

        public string Name { get; init; }

        public string ChangeVector { get; init; }
    }

    public sealed class ConflictSnapshot
    {
        public string Name { get; init; }

        public string ChangeVector { get; init; }

        public long Etag { get; init; }
    }

    public sealed class RevisionSnapshot
    {
        public bool Exists { get; init; }

        public string Name { get; init; }

        public string ChangeVector { get; init; }

        public long Count { get; init; }
    }

    public sealed class RevisionTombstoneSnapshot
    {
        public string RawKey { get; init; }

        public string KeyChangeVector { get; init; }

        public string ChangeVector { get; init; }

        public long Etag { get; init; }
    }

    public sealed class TombstoneSnapshot
    {
        public bool Exists { get; init; }

        public string ChangeVector { get; init; }
    }

    public sealed class CounterSnapshot
    {
        public bool Exists { get; init; }

        public long Value { get; init; }
    }

    public sealed class AttachmentSnapshot
    {
        public bool Exists { get; init; }

        public string ChangeVector { get; init; }

        public string Hash { get; init; }

        public string ContentType { get; init; }

        public long Size { get; init; }
    }

    public sealed class AttachmentTombstoneSnapshot
    {
        public bool Exists { get; init; }

        public string ChangeVector { get; init; }
    }

    public sealed class TimeSeriesSegmentSnapshot
    {
        public bool Exists { get; init; }

        public string ChangeVector { get; init; }

        public long Etag { get; init; }

        public int ValueCount { get; init; }
    }

    public sealed class TimeSeriesDeletedRangeSnapshot
    {
        public bool Exists { get; init; }

        public string ChangeVector { get; init; }

        public DateTime From { get; init; }

        public DateTime To { get; init; }

        public long Etag { get; init; }
    }

    protected async Task<DualClusterLab> CreateDualClusterLabAsync(
        Options options,
        ClusterSide? filteredPassReceiveSide = null,
        string itemName = null)
    {
        var hubCluster = await CreateTaggedClusterAsync(nodeTags: ["HA", "HB", "HC"]);
        var sinkCluster = await CreateTaggedClusterAsync(nodeTags: ["SA", "SB", "SC"]);
        var hubAdminCertificate = RegisterClusterAdminCertificate(hubCluster);
        var sinkAdminCertificate = RegisterClusterAdminCertificate(sinkCluster);

        var hubDatabaseName = GetDatabaseName();
        var sinkDatabaseName = GetDatabaseName();

        var hubRecord = new DatabaseRecord(hubDatabaseName);
        DisableResolveToLatest(hubRecord);
        await CreateDatabaseInCluster(hubRecord, replicationFactor: 3, leadersUrl: hubCluster.Leader.WebUrl, certificate: hubAdminCertificate);

        var sinkRecord = new DatabaseRecord(sinkDatabaseName);
        DisableResolveToLatest(sinkRecord);
        await CreateDatabaseInCluster(sinkRecord, replicationFactor: 3, leadersUrl: sinkCluster.Leader.WebUrl, certificate: sinkAdminCertificate);

        var hubStores = Cluster.GetDocumentStores(
            hubCluster.Nodes,
            hubDatabaseName,
            disableTopologyUpdates: true,
            certificate: hubAdminCertificate);

        var sinkStores = Cluster.GetDocumentStores(
            sinkCluster.Nodes,
            sinkDatabaseName,
            disableTopologyUpdates: true,
            certificate: sinkAdminCertificate);

        var pullCertificate = Convert.ToBase64String(hubCluster.Certificates.ClientCertificate2.Value.Export(X509ContentType.Pfx));

        var lab = new DualClusterLab(hubCluster, sinkCluster, hubDatabaseName, pullCertificate, filteredPassReceiveSide, itemName);

        foreach (var node in NodesOnEachClusterSide)
        {
            var hubServer = hubCluster.Nodes.Single(x => string.Equals(x.ServerStore.NodeTag, NodeTag(ClusterSide.Hub, node), StringComparison.OrdinalIgnoreCase));
            var hubStore = hubStores[node switch { LabNode.A => 0, LabNode.B => 1, _ => 2 }];
            lab.AddNode(ClusterSide.Hub, node, hubServer, hubStore, await GetDocumentDatabaseInstanceForAsync(hubDatabaseName, hubServer));

            var sinkServer = sinkCluster.Nodes.Single(x => string.Equals(x.ServerStore.NodeTag, NodeTag(ClusterSide.Sink, node), StringComparison.OrdinalIgnoreCase));
            var sinkStore = sinkStores[node switch { LabNode.A => 0, LabNode.B => 1, _ => 2 }];
            lab.AddNode(ClusterSide.Sink, node, sinkServer, sinkStore, await GetDocumentDatabaseInstanceForAsync(sinkDatabaseName, sinkServer));
        }

        await lab.ConfigurePerNodeHubDefinitionsAsync();
        await lab.SeedInternalReplicationAsync();
        await lab.WaitForInternalHandlersAsync();
        await lab.ConfigureFilteredRoundTripReplicationAsync(filteredPassReceiveSide);

        return lab;
    }

    protected internal static string NodeTag(ClusterSide side, LabNode node)
    {
        var prefix = side == ClusterSide.Hub ? "H" : "S";
        return prefix + node;
    }

    protected internal static string NodeTagLower(ClusterSide side, LabNode node) => NodeTag(side, node).ToLowerInvariant();

    protected internal static string HubName(LabNode hubNode) => "Hub-" + NodeTag(ClusterSide.Hub, hubNode);

    protected internal new static Task<T> WaitForValueAsync<T>(
        Func<T> act,
        T expectedVal,
        int timeout = 15000,
        int interval = 100) =>
        ReplicationTestBase.WaitForValueAsync(act, expectedVal, timeout, interval);

    private X509Certificate2 RegisterClusterAdminCertificate(TaggedCluster cluster) =>
        Certificates.RegisterClientCertificate(
            cluster.Certificates.ServerCertificateForCommunication.Value,
            cluster.Certificates.ClientCertificate1.Value,
            new Dictionary<string, DatabaseAccess>(),
            SecurityClearance.ClusterAdmin,
            server: cluster.Leader);

    protected internal static string FormatRevisionTombstones(List<RevisionTombstoneSnapshot> tombstones) =>
        tombstones.Count == 0
            ? "<none>"
            : string.Join("; ", tombstones.Select(snapshot => $"etag={snapshot.Etag}, rawKey='{snapshot.RawKey}', keyCV='{snapshot.KeyChangeVector}', CV='{snapshot.ChangeVector}'"));

    protected internal static string FormatConflicts(List<ConflictSnapshot> conflicts) =>
        conflicts.Count == 0
            ? "<none>"
            : string.Join("; ", conflicts.Select(x => $"etag={x.Etag}, name='{x.Name ?? "<null>"}', CV='{x.ChangeVector}'"));

    private async Task<TaggedCluster> CreateTaggedClusterAsync(string[] nodeTags)
    {
        var nodes = new List<RavenServer>();
        var urls = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        RavenServer leader = null;
        TestCertificatesHolder certificates = null;

        foreach (var nodeTag in nodeTags)
        {
            var settings = new Dictionary<string, string>(DefaultClusterSettings)
            {
                [RavenConfiguration.GetKey(configuration => configuration.Cluster.ElectionTimeout)] = "300"
            };

            var serverUrl = UseFiddlerUrl("https://127.0.0.1:0");
            certificates = Certificates.SetupServerAuthentication(settings, serverUrl, certificates, with2Eku: true);

            var server = GetNewServer(
                new ServerCreationOptions
                {
                    CustomSettings = settings,
                    RunInMemory = true,
                    RegisterForDisposal = false,
                    NodeTag = nodeTag
                });

            Servers.Add(server);
            nodes.Add(server);
            urls[nodeTag] = UseFiddlerUrl(server.WebUrl);

            if (leader != null)
                continue;

            await server.ServerStore.EnsureNotPassiveAsync(nodeTag: nodeTag);
            leader = server;
        }

        for (var i = 1; i < nodes.Count; i++)
        {
            var followerServer = nodes[i];
            var followerTag = nodeTags[i];

            leader = await ActionWithLeader(
                currentLeader => currentLeader.ServerStore.AddNodeToClusterAsync(
                    urls[followerTag],
                    nodeTag: followerTag,
                    asWatcher: true,
                    token: CancellationToken.None),
                nodes);

            await followerServer.ServerStore.WaitForTopology(Leader.TopologyModification.NonVoter, CancellationToken.None);

            leader.ServerStore.Engine.GetLastCommitIndex(out var index, out _);
            await followerServer.ServerStore.WaitForCommitIndexChange(RachisConsensus.CommitIndexModification.GreaterOrEqual, index);
        }

        foreach (var ravenServerNode in nodes)
        {
            var nodesInTopology = await WaitForValueAsync(
                () => ravenServerNode.ServerStore.GetClusterTopology().AllNodes.Count,
                expectedVal: nodeTags.Length,
                timeout: 30_000,
                interval: 100);

            Assert.Equal(nodeTags.Length, nodesInTopology);
        }

        Assert.NotNull(leader);
        var isLeaderReady = await leader.ServerStore.WaitForState(RachisState.Leader, CancellationToken.None)
            .WaitWithoutExceptionAsync(TimeSpan.FromSeconds(30));

        Assert.True(isLeaderReady, $"Expected {leader.ServerStore.NodeTag} to stay leader after creating tagged cluster.");

        return new TaggedCluster(nodes, leader, certificates);
    }

    private static void DisableResolveToLatest(DatabaseRecord record)
    {
        record.ConflictSolverConfig = new ConflictSolver { ResolveToLatest = false };
    }
}
