using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.ServerWide;
using Raven.Server.Config;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_18515 : ReplicationTestBase
    {
        public RavenDB_18515(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task Can_Set_Pin_To_Node_Pull_Replication_As_Sink()
        {
            var (nodes, leader, certificates) = await CreateRaftClusterWithSsl(3, watcherCluster: true, useReservedPorts: true);

            var followers = nodes.Where(s => s.ServerStore.NodeTag != leader.ServerStore.NodeTag).ToList();

            var hubDatabaseName = GetDatabaseName();
            var sinkDatabaseName = GetDatabaseName();

            var hubMentorNode = followers[0];
            var sinkMentorNode = followers[1];
            using var hubStore = GetDocumentStore(new Options
            {
                Server = hubMentorNode,
                ReplicationFactor = 1,
                ModifyDatabaseName = (name) => hubDatabaseName,
                ClientCertificate = certificates.ServerCertificate.Value,
                AdminCertificate = certificates.ServerCertificate.Value,
                ModifyDatabaseRecord = r =>
                {
                    r.Topology = new DatabaseTopology();
                    r.Topology.Members.Add(hubMentorNode.ServerStore.NodeTag);
                } // database is created on a random node (chosen in 'AssignNodesToDatabase' method), so we force it to be created on the hubMentorNode
            });

            using var sinkStore = GetDocumentStore(new Options
            {
                Server = leader,
                ModifyDatabaseName = (name) => sinkDatabaseName,
                ReplicationFactor = 3,
                ClientCertificate = certificates.ServerCertificate.Value,
                AdminCertificate = certificates.ServerCertificate.Value,
            });

            var saveResult = await hubStore.Maintenance.SendAsync(new PutPullReplicationAsHubOperation(new PullReplicationDefinition
            {
                Name = "HUB",
                Mode = PullReplicationMode.SinkToHub
            }));

            await hubStore.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation("HUB",
                new ReplicationHubAccess
                {
                    Name = hubStore.Database,
                    CertificateBase64 = Convert.ToBase64String(certificates.ClientCertificate1.Value.Export(X509ContentType.Cert)),
                }));

            await sinkStore.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(new RavenConnectionString
            {
                Database = hubStore.Database,
                Name = "SINK",
                TopologyDiscoveryUrls = hubStore.Urls
            }));

            await sinkStore.Maintenance.SendAsync(new UpdatePullReplicationAsSinkOperation(new PullReplicationAsSink
            {
                ConnectionStringName = "SINK",
                PinToMentorNode = true,
                CertificateWithPrivateKey = Convert.ToBase64String(certificates.ClientCertificate1.Value.Export(X509ContentType.Pfx)),
                // TaskId = saveResult.TaskId,
                MentorNode = sinkMentorNode.ServerStore.NodeTag,
                Mode = PullReplicationMode.SinkToHub,
                HubName = "HUB",
            }));

            using (var sinkSession = sinkStore.OpenSession())
            {
                sinkSession.Store(new User { Id = "users/1", Name = "Arava", });
                sinkSession.SaveChanges();
            }

            Assert.True(WaitForDocument<User>(hubStore, "users/1", u => u.Name == "Arava", 10_000));
            var disposedServer = await DisposeServerAndWaitForFinishOfDisposalAsync(sinkMentorNode);
            using (var sinkSession = sinkStore.OpenSession())
            {
                sinkSession.Store(new User { Id = "users/2", Name = "Arava2", });
                sinkSession.SaveChanges();
            }
            
            Assert.False(WaitForDocument<User>(hubStore, "users/2", u => u.Name == "Arava2", 10_000));
            var revivedServer = GetNewServer(new ServerCreationOptions
            {
                CustomSettings = new Dictionary<string, string>
                {
                    { RavenConfiguration.GetKey(x => x.Core.ServerUrls), disposedServer.Url },
                    { RavenConfiguration.GetKey(x => x.Security.CertificatePath), certificates.ServerCertificatePath }
                },
                RunInMemory = true,
                DataDirectory = disposedServer.DataDirectory,
                DeletePrevious = false
            });
            Assert.True(WaitForDocument<User>(sinkStore, "users/2", u => u.Name == "Arava2", 10_000));
            Assert.Equal(1, await WaitForValueAsync(async () => await GetMembersCount(hubStore, hubStore.Database), 1));
            Assert.Equal(3, await WaitForValueAsync(async () => await GetMembersCount(sinkStore, sinkStore.Database), 3));

        }


        private class User
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }
    }
}
