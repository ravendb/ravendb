using System.Collections.Generic;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Server;
using Raven.Server.NotificationCenter.Handlers;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_17006 : ReplicationTestBase
    {
        public RavenDB_17006(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task CanConnectToRemoteNodeFromClusterDashboardWhenUsingSelfSignedCertificate()
        {
            var clusterSize = 2;

            var result = await CreateRaftClusterWithSsl(clusterSize, false);

            RavenServer leader = result.Leader;

            X509Certificate2 clientCertificate = Certificates.RegisterClientCertificate(result.Certificates.ServerCertificate.Value, result.Certificates.ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin, server: leader);

            var localNode = result.Nodes[0];
            var remoteNode = result.Nodes[1];

            string remoteNodeUrl = remoteNode.ServerStore.GetNodeHttpServerUrl();

            using (var connection = new ProxyWebSocketConnection(null, remoteNodeUrl, $"/admin/cluster-dashboard/remote/watch?thumbprint={clientCertificate.Thumbprint}", localNode.ServerStore.ContextPool, localNode.ServerStore.ServerShutdown))
            {
                await connection.Establish(Server.Certificate?.Certificate);
            }
        }
    }
}
