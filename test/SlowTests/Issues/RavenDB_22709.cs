using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Client.Util;
using Raven.Server;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_22709 : ReplicationTestBase
    {
        public RavenDB_22709(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Replication)]
        public async Task PullReplicationWithSinksWithSameDatabaseNameShouldWork()
        {
            var hubCluster = await CreateRaftClusterWithSsl(3, watcherCluster: true, leaderIndex: 0);
            var hubClusterCert = RegisterClientCertificate(hubCluster);

            var sinkCluster = await CreateRaftClusterWithSsl(1);
            var sinkClusterCert = RegisterClientCertificate(sinkCluster);

            var sinkCluster2 = await CreateRaftClusterWithSsl(1);
            var sinkClusterCert2 = RegisterClientCertificate(sinkCluster2);

            using var hubStore = new DocumentStore
            {
                Urls = new[] { hubCluster.Leader.WebUrl },
                Database = "HubDB",
                Certificate = hubClusterCert,
                Conventions = { DisableTopologyUpdates = true, DisposeCertificate = false }
            }.Initialize();

            using var sinkStore1 = GetDocumentStore(new Options
            {
                Server = sinkCluster.Leader,
                AdminCertificate = sinkClusterCert,
                ClientCertificate = sinkClusterCert,
                ModifyDatabaseName = s => "SinkDB"
            });

            using var sinkStore2 = GetDocumentStore(new Options
            {
                Server = sinkCluster2.Leader,
                AdminCertificate = sinkClusterCert2,
                ClientCertificate = sinkClusterCert2,
                ModifyDatabaseName = s => "SinkDB"
            });

            await CreateDatabaseInCluster(hubStore.Database, replicationFactor: 3, leadersUrl: hubCluster.Leader.WebUrl, certificate: hubClusterCert);

            var pullCert1 = CertificateHelper.CreateCertificate(await File.ReadAllBytesAsync(hubCluster.Certificates.ClientCertificate2Path), (string)null, X509KeyStorageFlags.Exportable);
            var pullCert2 = CertificateHelper.CreateCertificate(await File.ReadAllBytesAsync(hubCluster.Certificates.ClientCertificate3Path), (string)null, X509KeyStorageFlags.Exportable);

            await hubStore.Maintenance.SendAsync(new PutPullReplicationAsHubOperation(new PullReplicationDefinition
            {
                Name = "both",
                Mode = PullReplicationMode.SinkToHub | PullReplicationMode.HubToSink,
                WithFiltering = true,
                PinToMentorNode = true,
                MentorNode = hubCluster.Leader.ServerStore.NodeTag
            }));

            await hubStore.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation("both", new ReplicationHubAccess
            {
                Name = "Sink1",
                AllowedSinkToHubPaths = new[] { "*" },
                AllowedHubToSinkPaths = new[] { "*" },
                CertificateBase64 = Convert.ToBase64String(pullCert1.Export(X509ContentType.Cert)),
            }));

            await SetupSink(sinkStore1, hubStore, "Sink1", pullCert1);

            await hubStore.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation("both", new ReplicationHubAccess
            {
                Name = "Sink2",
                AllowedSinkToHubPaths = new[] { "*" },
                AllowedHubToSinkPaths = new[] { "*" },
                CertificateBase64 = Convert.ToBase64String(pullCert2.Export(X509ContentType.Cert)),
            }));

            await SetupSink(sinkStore2, hubStore, "Sink2", pullCert2);

            // 2 outgoing internal replication connections 
            // 2 outgoing pull replication connections

            // 2 incoming internal replication connections 
            // 2 incoming pull replication connections

            var expectedConnections = 4;

            await AssertOutgoingConnections(hubStore, expectedConnections);
            await AssertIncomingConnections(hubStore, expectedConnections);
        }

        [RavenFact(RavenTestCategory.Replication)]
        public async Task ShouldRemoveConnectionsAfterUpdatingMentorNodeForHubTask()
        {
            var hubCluster = await CreateRaftClusterWithSsl(3, watcherCluster: true, leaderIndex: 0);
            var hubClusterCert = RegisterClientCertificate(hubCluster);

            var sinkCluster = await CreateRaftClusterWithSsl(3, watcherCluster: true, leaderIndex: 0);
            var sinkClusterCert = RegisterClientCertificate(sinkCluster);

            using var hubStore = new DocumentStore
            {
                Urls = new[] { hubCluster.Leader.WebUrl },
                Database = "HubDB",
                Certificate = hubClusterCert,
                Conventions = { DisableTopologyUpdates = true, DisposeCertificate = false }
            }.Initialize();

            using var sinkStore = new DocumentStore
            {
                Urls = new[] { sinkCluster.Leader.WebUrl },
                Database = "SinkDB",
                Certificate = sinkClusterCert,
                Conventions = { DisableTopologyUpdates = true, DisposeCertificate = false }
            }.Initialize();

            await CreateDatabaseInCluster(hubStore.Database, replicationFactor: 3, leadersUrl: hubCluster.Leader.WebUrl, certificate: hubClusterCert);
            await CreateDatabaseInCluster(sinkStore.Database, replicationFactor: 3, leadersUrl: sinkCluster.Leader.WebUrl, certificate: sinkClusterCert);

            var pullCert = CertificateHelper.CreateCertificate(await File.ReadAllBytesAsync(hubCluster.Certificates.ClientCertificate2Path), (string)null, X509KeyStorageFlags.Exportable);

            var pullDefinition = new PullReplicationDefinition
            {
                Name = "both",
                Mode = PullReplicationMode.SinkToHub | PullReplicationMode.HubToSink,
                WithFiltering = true,
                PinToMentorNode = true,
                MentorNode = hubCluster.Leader.ServerStore.NodeTag
            };

            var result = await hubStore.Maintenance.SendAsync(new PutPullReplicationAsHubOperation(pullDefinition));

            await hubStore.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation("both", new ReplicationHubAccess
            {
                Name = "Sink",
                AllowedSinkToHubPaths = new[] { "*" },
                AllowedHubToSinkPaths = new[] { "*" },
                CertificateBase64 = Convert.ToBase64String(pullCert.Export(X509ContentType.Cert)),
            }));

            await SetupSink(sinkStore, hubStore, "Sink", pullCert);

            // 2 outgoing internal replication connections
            // 1 outgoing pull replication connections

            // 2 incoming internal replication connections
            // 1 incoming pull replication connections

            var expectedConnections = 3;

            await AssertOutgoingConnections(hubStore, expectedConnections);
            await AssertIncomingConnections(hubStore, expectedConnections);

            pullDefinition.TaskId = result.TaskId;

            await hubStore.Maintenance.SendAsync(new ToggleOngoingTaskStateOperation(pullDefinition.TaskId, OngoingTaskType.PullReplicationAsHub, disable: true));

            pullDefinition.Disabled = false;
            pullDefinition.MentorNode = hubCluster.Nodes.First(s => s.ServerStore.NodeTag != hubCluster.Leader.ServerStore.NodeTag).ServerStore.NodeTag;

            await hubStore.Maintenance.SendAsync(new PutPullReplicationAsHubOperation(pullDefinition));

            foreach (var server in hubCluster.Nodes)
            {
                using (var store = new DocumentStore
                {
                    Urls = new[] { server.WebUrl },
                    Database = "HubDB",
                    Certificate = hubClusterCert,
                    Conventions = { DisableTopologyUpdates = true, DisposeCertificate = false }
                }.Initialize())
                {
                    if (server.ServerStore.NodeTag != pullDefinition.MentorNode)
                    {
                        // 2 outgoing internal replication connections

                        // 2 incoming internal replication connections

                        expectedConnections = 2;
                    }
                    else
                    {
                        // 2 outgoing internal replication connections
                        // 1 outgoing pull replication connections

                        // 2 incoming internal replication connections
                        // 1 incoming pull replication connections

                        expectedConnections = 3;
                    }

                    await AssertOutgoingConnections(store, expectedConnections);
                    await AssertIncomingConnections(store, expectedConnections);
                }
            }
        }

        [RavenFact(RavenTestCategory.Replication)]
        public async Task ShouldRemoveConnectionsAfterUpdatingResponsibleNodeForSinkTask()
        {
            var hubCluster = await CreateRaftClusterWithSsl(1);
            var hubClusterCert = RegisterClientCertificate(hubCluster);

            var sinkCluster = await CreateRaftClusterWithSsl(3, watcherCluster: true, leaderIndex: 0);
            var sinkClusterCert = RegisterClientCertificate(sinkCluster);

            using var hubStore = new DocumentStore
            {
                Urls = new[] { hubCluster.Leader.WebUrl },
                Database = "HubDB",
                Certificate = hubClusterCert,
                Conventions = { DisableTopologyUpdates = true, DisposeCertificate = false }
            }.Initialize();

            using var sinkStore = new DocumentStore
            {
                Urls = new[] { sinkCluster.Leader.WebUrl },
                Database = "SinkDB",
                Certificate = sinkClusterCert,
                Conventions = { DisposeCertificate = false }
            }.Initialize();

            await CreateDatabaseInCluster(hubStore.Database, replicationFactor: 1, leadersUrl: hubCluster.Leader.WebUrl, certificate: hubClusterCert);
            await CreateDatabaseInCluster(sinkStore.Database, replicationFactor: 3, leadersUrl: sinkCluster.Leader.WebUrl, certificate: sinkClusterCert);

            var pullCert = CertificateHelper.CreateCertificate(await File.ReadAllBytesAsync(hubCluster.Certificates.ClientCertificate2Path), (string)null, X509KeyStorageFlags.Exportable);

            await hubStore.Maintenance.SendAsync(new PutPullReplicationAsHubOperation(new PullReplicationDefinition
            {
                Name = "both",
                Mode = PullReplicationMode.SinkToHub | PullReplicationMode.HubToSink,
                WithFiltering = true,
                PinToMentorNode = true,
                MentorNode = hubCluster.Leader.ServerStore.NodeTag
            }));

            await hubStore.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation("both", new ReplicationHubAccess
            {
                Name = "Sink",
                AllowedSinkToHubPaths = new[] { "*" },
                AllowedHubToSinkPaths = new[] { "*" },
                CertificateBase64 = Convert.ToBase64String(pullCert.Export(X509ContentType.Cert)),
            }));

            await sinkStore.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(new RavenConnectionString
            {
                Database = hubStore.Database,
                Name = hubStore.Database + "ConStr",
                TopologyDiscoveryUrls = hubStore.Urls
            }));

            var sinkReplication = new PullReplicationAsSink
            {
                PinToMentorNode = true,
                MentorNode = sinkCluster.Leader.ServerStore.NodeTag,
                ConnectionStringName = hubStore.Database + "ConStr",
                Mode = PullReplicationMode.SinkToHub | PullReplicationMode.HubToSink,
                CertificateWithPrivateKey = Convert.ToBase64String(pullCert.Export(X509ContentType.Pfx)),
                HubName = "both",
                AccessName = "Sink",
                AllowedHubToSinkPaths = new[] { "*" },
                AllowedSinkToHubPaths = new[] { "*" }
            };

            var result = await sinkStore.Maintenance.SendAsync(new UpdatePullReplicationAsSinkOperation(sinkReplication));

            // 1 outgoing pull replication connections

            // 1 incoming pull replication connections

            var expectedConnections = 1;

            await AssertOutgoingConnections(hubStore, expectedConnections);
            await AssertIncomingConnections(hubStore, expectedConnections);

            sinkReplication.TaskId = result.TaskId;
            sinkReplication.MentorNode = sinkCluster.Nodes.First(s => s.WebUrl != sinkCluster.Leader.WebUrl).ServerStore.NodeTag;

            await sinkStore.Maintenance.SendAsync(new UpdatePullReplicationAsSinkOperation(sinkReplication));

            foreach (var server in sinkCluster.Nodes)
            {
                using (var store = new DocumentStore
                {
                    Urls = new[] { server.WebUrl },
                    Database = "SinkDB",
                    Certificate = sinkClusterCert,
                    Conventions = { DisableTopologyUpdates = true, DisposeCertificate = false }
                }.Initialize())
                {
                    if (server.ServerStore.NodeTag != sinkReplication.MentorNode)
                    {
                        // 2 outgoing internal replication connections 

                        // 2 incoming internal replication connections

                        expectedConnections = 2;
                    }
                    else
                    {
                        // 2 outgoing internal replication connections
                        // 1 outgoing pull replication connections

                        // 2 incoming internal replication connections
                        // 1 incoming pull replication connections

                        expectedConnections = 3;
                    }

                    await AssertOutgoingConnections(store, expectedConnections);
                    await AssertIncomingConnections(store, expectedConnections);
                }
            }
        }

        private X509Certificate2 RegisterClientCertificate((List<RavenServer> Nodes, RavenServer Leader, TestCertificatesHolder Certificates) cluster)
        {
            return Certificates.RegisterClientCertificate(cluster.Certificates.ServerCertificate.Value, cluster.Certificates
                .ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin, server: cluster.Leader);
        }

        private async Task SetupSink(IDocumentStore sinkStore, IDocumentStore hubStore, string accessName, X509Certificate2 pullCert)
        {
            await sinkStore.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(new RavenConnectionString
            {
                Database = hubStore.Database,
                Name = hubStore.Database + "ConStr",
                TopologyDiscoveryUrls = hubStore.Urls
            }));
            await sinkStore.Maintenance.SendAsync(new UpdatePullReplicationAsSinkOperation(new PullReplicationAsSink
            {
                ConnectionStringName = hubStore.Database + "ConStr",
                Mode = PullReplicationMode.SinkToHub | PullReplicationMode.HubToSink,
                CertificateWithPrivateKey = Convert.ToBase64String(pullCert.Export(X509ContentType.Pfx)),
                HubName = "both",
                AccessName = accessName,
                AllowedHubToSinkPaths = new[] { "*" },
                AllowedSinkToHubPaths = new[] { "*" }
            }));
        }

        private async Task AssertOutgoingConnections(IDocumentStore store, int expectedConnections)
        {
            Assert.Equal(expectedConnections, await WaitForValueAsync(async () =>
            {
                var stats = await store.Maintenance.SendAsync(new GetReplicationPerformanceStatisticsOperation());
                return stats.Outgoing.Length;
            }, expectedConnections));
        }

        private async Task AssertIncomingConnections(IDocumentStore store, int expectedConnections)
        {
            Assert.Equal(expectedConnections, await WaitForValueAsync(async () =>
            {
                var stats = await store.Maintenance.SendAsync(new GetReplicationPerformanceStatisticsOperation());
                return stats.Incoming.Length;
            }, expectedConnections));
        }
    }
}
