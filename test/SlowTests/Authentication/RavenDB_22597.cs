// Reproduction test for https://github.com/ravendb/ravendb/issues/22597
//
// Bug: after a cluster node is rebuilt with fresh storage at the same URL and re-added to the
// cluster, clients that connect before the cert is synced get a persistent 403
// "The supplied client certificate is unknown to the server" error.
//
// Root cause: when a fresh node joins and the leader's log has been compacted, the leader sends
// a FULL Raft snapshot.  The snapshot writes cert data to Voron but does NOT fire
// OnValueChanged("PutCertificateCommand"), so ServerStore.LastCertificateUpdateTime is never
// set.  ShouldRetryToAuthenticateConnection requires LastCertificateUpdateTime.HasValue == true
// before it will re-authenticate an existing TCP connection, so the cached
// UnfamiliarCertificate status on that connection is never cleared → 403 forever.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Client.ServerWide;
using Raven.Server.Rachis;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Authentication
{
    public class RavenDB_22597 : ReplicationTestBase
    {
        public RavenDB_22597(ITestOutputHelper output) : base(output) { }

        [RavenFact(RavenTestCategory.Security | RavenTestCategory.Certificates | RavenTestCategory.Cluster)]
        public async Task ClientShouldNotGetAuthErrorAfterNodeReplacedWithSameUrl()
        {
            // 2-node SSL cluster: leader + one follower (the node to be replaced).
            var (nodes, leader, certs) = await CreateRaftClusterWithSsl(2, shouldRunInMemory: false, watcherCluster: false);

            // Register a cluster-admin client cert; stored via PutCertificateCommand and
            // replicated to both nodes through individual Raft log entries.
            var adminCert = Certificates.RegisterClientCertificate(
                certs,
                new Dictionary<string, DatabaseAccess>(),
                SecurityClearance.ClusterAdmin,
                server: leader);

            var nodeToReplace = nodes.First(n => n != leader);
            var replacedTag = nodeToReplace.ServerStore.NodeTag;
            var replacedUrl = nodeToReplace.WebUrl;

            // Wait for the cert to replicate to both nodes.
            await nodeToReplace.ServerStore.WaitForCommitIndexChange(
                RachisConsensus.CommitIndexModification.GreaterOrEqual,
                leader.ServerStore.LastRaftCommitIndex);

            // Remove follower from cluster, dispose, and wipe its storage.
            await ActionWithLeader(l => l.ServerStore.RemoveFromClusterAsync(replacedTag));
            Assert.True(
                await nodeToReplace.ServerStore
                    .WaitForState(RachisState.Passive, CancellationToken.None)
                    .WaitWithoutExceptionAsync(TimeSpan.FromSeconds(30)),
                $"Node {replacedTag} did not become passive");
            await DisposeServerAndWaitForFinishOfDisposalAsync(nodeToReplace);

            // Compact the leader's Raft log so the replacement node receives a FULL snapshot
            // when it joins, rather than individual log entries.  In production clusters the
            // log is compacted continuously, so a fresh node always joins via full snapshot.
            using (leader.ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext ctx))
            using (var tx = ctx.OpenWriteTransaction())
            {
                leader.ServerStore.Engine.GetLastCommitIndex(ctx, out long commitIndex, out _);
                leader.ServerStore.Engine.TruncateLogBefore(ctx, commitIndex);
                tx.Commit();
            }

            // Start a fresh replacement node at the same URL with the same cluster certificate
            // but completely empty storage — simulates a rebuilt/replaced machine.
            var customSettings = new Dictionary<string, string>(DefaultClusterSettings);
            Certificates.SetupServerAuthentication(customSettings, replacedUrl, certs);
            var replacementNode = GetNewServer(new ServerCreationOptions
            {
                RunInMemory = false,
                DeletePrevious = true,
                RegisterForDisposal = true,
                CustomSettings = customSettings
            });
            await replacementNode.ServerStore.InitializationCompleted.WaitAsync();

            // Connect to the replacement node BEFORE it joins the cluster.
            // Maintenance.Server caches one ClusterRequestExecutor (and thus one HttpClient)
            // for the store's lifetime, so both requests below share the same TCP connection.
            using var directStore = new DocumentStore
            {
                Urls = new[] { replacedUrl },
                Certificate = adminCert,
                Conventions = { DisposeCertificate = false, DisableTopologyUpdates = true }
            }.Initialize();

            // First request: establishes the TCP connection to the new node.
            // The cert is not yet in its state machine → 403, UnfamiliarCertificate cached
            // on the connection.
            await Assert.ThrowsAnyAsync<Exception>(() =>
                directStore.Maintenance.Server.SendAsync(new GetDatabaseNamesOperation(0, 25)));

            // Re-add the replacement node.  The leader sends a FULL snapshot (log was compacted).
            // The snapshot installs the cert in Voron but does NOT call
            // OnValueChanged("PutCertificateCommand"), so LastCertificateUpdateTime stays null.
            await ActionWithLeader(l => l.ServerStore.AddNodeToClusterAsync(replacedUrl, replacedTag));
            Assert.True(
                await replacementNode.ServerStore
                    .WaitForState(RachisState.Follower, CancellationToken.None)
                    .WaitWithoutExceptionAsync(TimeSpan.FromSeconds(30)),
                "Replacement node did not join cluster as follower");

            // Wait until the replacement node has committed the full snapshot.
            await replacementNode.ServerStore.WaitForCommitIndexChange(
                RachisConsensus.CommitIndexModification.GreaterOrEqual,
                leader.ServerStore.LastRaftCommitIndex);

            // The cert IS now in the replacement node's state machine.
            // However ShouldRetryToAuthenticateConnection returns false because
            // LastCertificateUpdateTime is null — so the existing TCP connection still carries
            // the cached UnfamiliarCertificate status and every request on it gets 403.
            //
            // On the buggy build this throws AuthorizationException ("unknown certificate").
            // After the fix it should succeed.
            var names = await directStore.Maintenance.Server.SendAsync(new GetDatabaseNamesOperation(0, 25));
            Assert.NotNull(names);
        }
    }
}
