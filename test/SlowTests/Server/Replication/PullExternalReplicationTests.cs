using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using Esprima.Ast;
using FastTests.Server.Replication;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Session;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Server;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Json;
using Xunit;

namespace SlowTests.Server.Replication
{
    public class PullExternalReplicationTests : ReplicationTestBase
    {
        [Fact]
        public async Task CanDefinePullReplication()
        {
            using (var store = GetDocumentStore())
            {
                await store.Maintenance.ForDatabase(store.Database).SendAsync(new UpdatePullReplicationDefinitionOperation("test"));
            }
        }

        [Fact]
        public async Task PullExternalReplicationShouldWork()
        {
            var name = $"pull-replication {GetDatabaseName()}";
            using (var store1 = GetDocumentStore())
            using (var store2 = GetDocumentStore())
            {
                await store2.Maintenance.ForDatabase(store2.Database).SendAsync(new UpdatePullReplicationDefinitionOperation(name));
                using (var s2 = store2.OpenSession())
                {
                    s2.Store(new User(), "foo/bar");
                    s2.SaveChanges();
                }

                await SetupPullReplicationAsync(name, store1, store2);

                var timeout = 3000;
                Assert.True(WaitForDocument(store1, "foo/bar", timeout), store1.Identifier);
            }
        }

        [Fact]
        public async Task MultiplePullExternalReplicationShouldWork()
        {
            var name = $"pull-replication {GetDatabaseName()}";
            using (var central = GetDocumentStore())
            using (var edge1 = GetDocumentStore())
            using (var edge2 = GetDocumentStore())
            {
                await central.Maintenance.ForDatabase(central.Database).SendAsync(new UpdatePullReplicationDefinitionOperation(name));
                using (var session = central.OpenSession())
                {
                    session.Store(new User(), "foo/bar");
                    session.SaveChanges();
                }

                await SetupPullReplicationAsync(name, edge1, central);
                await SetupPullReplicationAsync(name, edge2, central);

                var timeout = 3000;
                Assert.True(WaitForDocument(edge1, "foo/bar", timeout), edge1.Identifier);
                Assert.True(WaitForDocument(edge2, "foo/bar", timeout), edge2.Identifier);
            }
        }

        [Fact]
        public async Task PullExternalReplicationWithCertificateShouldWork()
        {
            var centralSettings = new ConcurrentDictionary<string, string>();
            var edgeSettings = new ConcurrentDictionary<string, string>();

            var centralCertPath = SetupServerAuthentication(centralSettings, createNew: true);
            var edgeCertPath = SetupServerAuthentication(edgeSettings, createNew: true);

            var centralDB = GetDatabaseName();
            var edgeDB = GetDatabaseName();
            var pullReplicationName = $"{centralDB}-pull";

            var centralServer = GetNewServer(centralSettings);
            var edgeServer = GetNewServer(edgeSettings);

            var centralAdminCert = new X509Certificate2(centralCertPath, (string)null, X509KeyStorageFlags.MachineKeySet);
            var edgeAdminCert = new X509Certificate2(edgeCertPath, (string)null, X509KeyStorageFlags.MachineKeySet);

            var dummy = GenerateAndSaveSelfSignedCertificate(true);
            var pullReplicationCertificate = new X509Certificate2(dummy, (string)null, X509KeyStorageFlags.MachineKeySet | X509KeyStorageFlags.Exportable);
            Assert.True(pullReplicationCertificate.HasPrivateKey);

            await PutCertificateInCentral(pullReplicationName, centralServer, centralAdminCert, centralDB, pullReplicationCertificate);

            using (var centralStore = GetDocumentStore(new Options
            {
                ClientCertificate = centralAdminCert,
                Server = centralServer,
                CreateDatabase = false,
                ModifyDatabaseName = _ => centralDB
            }))
            using (var edgeStore = GetDocumentStore(new Options
            {
                ClientCertificate = edgeAdminCert,
                Server = edgeServer,
                ModifyDatabaseName = _ => edgeDB
            }))
            {
                await edgeStore.Maintenance.SendAsync(new PutFeatureCertificateOperation("PullReplicationCertificate", pullReplicationCertificate));

                await SetupPullReplicationAsync(pullReplicationName, edgeStore, pullReplicationCertificate.Thumbprint, centralStore);
                using (var centralSession = centralStore.OpenSession())
                {
                    centralSession.Store(new User(), "foo/bar");
                    centralSession.SaveChanges();
                }

                var timeout = 3000;
                Assert.True(WaitForDocument(edgeStore, "foo/bar", timeout), edgeStore.Identifier);
            }
        }

        private async Task PutCertificateInCentral(string pullReplicationName, RavenServer server, X509Certificate2 centralAdminCert, string centralDB,
            X509Certificate2 certificate)
        {
            using (var store = GetDocumentStore(new Options
            {
                ClientCertificate = centralAdminCert,
                Server = server,
                ModifyDatabaseName = _=> centralDB
            }))
            {
                await store.Maintenance.ForDatabase(store.Database).SendAsync(new UpdatePullReplicationDefinitionOperation(new PullReplicationDefinition(pullReplicationName)
                {
                    Certificates = new List<string>(new[] { Convert.ToBase64String(certificate.Export(X509ContentType.Cert)) })
                }));
            }
        }

        [Fact]
        public async Task CentralFailover()
        {
            var clusterSize = 3;
            var central = await CreateRaftClusterAndGetLeader(clusterSize);
            var minion = await CreateRaftClusterAndGetLeader(clusterSize);

            var centralDB = GetDatabaseName();
            var minionDB = GetDatabaseName();

            var dstTopology = await CreateDatabaseInCluster(minionDB, clusterSize, minion.WebUrl);
            var srcTopology = await CreateDatabaseInCluster(centralDB, clusterSize, central.WebUrl);

            using (var centralStore = new DocumentStore
            {
                Urls = new[] { central.WebUrl },
                Database = centralDB
            }.Initialize())
            using (var minionStore = new DocumentStore
            {
                Urls = new[] { minion.WebUrl },
                Database = minionDB
            }.Initialize())
            {
                using (var session = centralStore.OpenSession())
                {
                    session.Advanced.WaitForReplicationAfterSaveChanges(timeout: TimeSpan.FromSeconds(10), replicas: clusterSize - 1);
                    session.Store(new User
                    {
                        Name = "Karmel"
                    }, "users/1");
                    session.SaveChanges();
                }

                var name = $"pull-replication {GetDatabaseName()}";
                await centralStore.Maintenance.ForDatabase(centralStore.Database).SendAsync(new UpdatePullReplicationDefinitionOperation(name));

                // add pull replication with invalid discovery url to test the failover on database topology discovery
                var pullReplication = new ExternalReplication(centralDB, $"ConnectionString-{centralDB}")
                {
                    PullReplicationAsEdgeOptions = new PullReplicationAsEdgeSettings(name),
                    MentorNode = "B", // this is the node were the data will be replicated to.
                };
                var urls = new List<string>();
                foreach (var ravenServer in srcTopology.Servers)
                {
                    urls.Add(ravenServer.WebUrl);
                }
                await AddWatcherToReplicationTopology((DocumentStore)minionStore, pullReplication, urls.ToArray());

                using (var dstSession = minionStore.OpenSession())
                {
                    Assert.True(await WaitForDocumentInClusterAsync<User>(
                        dstSession as DocumentSession,
                        "users/1",
                        u => u.Name.Equals("Karmel"),
                        TimeSpan.FromSeconds(30)));
                }

                var minionUrl = minion.ServerStore.GetClusterTopology().GetUrlFromTag("B");
                var server = Servers.Single(s => s.WebUrl == minionUrl);
                var handler = await InstantiateOutgoingTaskHandler(minionDB, server);
                Assert.True(WaitForValue(
                    () => handler.GetOngoingTasksInternal().OngoingTasksList.Single(t => t is OngoingTaskReplication).As<OngoingTaskReplication>().DestinationUrl !=
                          null,
                    true));

                var watcherTaskUrl = handler.GetOngoingTasksInternal().OngoingTasksList.Single(t => t is OngoingTaskReplication).As<OngoingTaskReplication>()
                    .DestinationUrl;

                // dispose the central node, from which we are currently pulling 
                DisposeServerAndWaitForFinishOfDisposal(Servers.Single(s => s.WebUrl == watcherTaskUrl));

                using (var session = centralStore.OpenSession())
                {
                    session.Advanced.WaitForReplicationAfterSaveChanges(timeout: TimeSpan.FromSeconds(10), replicas: clusterSize - 2);
                    session.Store(new User
                    {
                        Name = "Karmel2"
                    }, "users/2");
                    session.SaveChanges();
                }

                using (var dstSession = minionStore.OpenSession())
                {
                    Assert.True(await WaitForDocumentInClusterAsync<User>(
                        dstSession as DocumentSession,
                        "users/2",
                        u => u.Name.Equals("Karmel2"),
                        TimeSpan.FromSeconds(30)));
                }
            }
        }

        [Fact]
        public async Task EdgeFailover()
        {
            var clusterSize = 3;
            var central = await CreateRaftClusterAndGetLeader(clusterSize);
            var minion = await CreateRaftClusterAndGetLeader(clusterSize);

            var centralDB = GetDatabaseName();
            var minionDB = GetDatabaseName();

            var dstTopology = await CreateDatabaseInCluster(minionDB, clusterSize, minion.WebUrl);
            var srcTopology = await CreateDatabaseInCluster(centralDB, clusterSize, central.WebUrl);

            using (var centralStore = new DocumentStore
            {
                Urls = new[] { central.WebUrl },
                Database = centralDB
            }.Initialize())
            using (var minionStore = new DocumentStore
            {
                Urls = new[] { minion.WebUrl },
                Database = minionDB
            }.Initialize())
            {
                using (var session = centralStore.OpenSession())
                {
                    session.Advanced.WaitForReplicationAfterSaveChanges(timeout: TimeSpan.FromSeconds(10), replicas: clusterSize - 1);
                    session.Store(new User
                    {
                        Name = "Karmel"
                    }, "users/1");
                    session.SaveChanges();
                }

                var name = $"pull-replication {GetDatabaseName()}";
                await centralStore.Maintenance.ForDatabase(centralStore.Database).SendAsync(new UpdatePullReplicationDefinitionOperation(name));


                // add pull replication with invalid discovery url to test the failover on database topology discovery
                var pullReplication = new ExternalReplication(centralDB, $"ConnectionString-{centralDB}")
                {
                    PullReplicationAsEdgeOptions = new PullReplicationAsEdgeSettings(name),
                    MentorNode = "B", // this is the node were the data will be replicated to.
                };
                await AddWatcherToReplicationTopology((DocumentStore)minionStore, pullReplication, new[] { "http://127.0.0.1:1234", central.WebUrl });

                using (var dstSession = minionStore.OpenSession())
                {
                    Assert.True(await WaitForDocumentInClusterAsync<User>(
                        dstSession as DocumentSession,
                        "users/1",
                        u => u.Name.Equals("Karmel"),
                        TimeSpan.FromSeconds(30)));
                }

                var minionUrl = minion.ServerStore.GetClusterTopology().GetUrlFromTag("B");
                var server = Servers.Single(s => s.WebUrl == minionUrl);
                var handler = await InstantiateOutgoingTaskHandler(minionDB, server);
                Assert.True(WaitForValue(
                    () => handler.GetOngoingTasksInternal().OngoingTasksList.Single(t => t is OngoingTaskReplication).As<OngoingTaskReplication>().DestinationUrl !=
                          null,
                    true));

                // dispose the minion node.
                DisposeServerAndWaitForFinishOfDisposal(server);

                using (var session = centralStore.OpenSession())
                {
                    session.Advanced.WaitForReplicationAfterSaveChanges(timeout: TimeSpan.FromSeconds(10), replicas: clusterSize - 2);
                    session.Store(new User
                    {
                        Name = "Karmel2"
                    }, "users/2");
                    session.SaveChanges();
                }

                var user = WaitForDocumentToReplicate<User>(
                    minionStore,
                    "users/2",
                    30_000);

                Assert.Equal("Karmel2", user.Name);
            }
        }

        public Task<List<ModifyOngoingTaskResult>> SetupPullReplicationAsync(string remoteName, DocumentStore edge, params DocumentStore[] central)
        {
            return SetupPullReplicationAsync(remoteName, edge, null, central);
        }

        public async Task<List<ModifyOngoingTaskResult>> SetupPullReplicationAsync(string remoteName, DocumentStore edge, string thumbprint,params DocumentStore[] central)
        {
            var tasks = new List<Task<ModifyOngoingTaskResult>>();
            var resList = new List<ModifyOngoingTaskResult>();
            foreach (var store in central)
            {
                var databaseWatcher = new ExternalReplication(store.Database,$"ConnectionString-{store.Database}")
                {
                    PullReplicationAsEdgeOptions = new PullReplicationAsEdgeSettings(remoteName)
                    {
                        CertificateThumbprint = thumbprint
                    },
                };
                ModifyReplicationDestination(databaseWatcher);
                tasks.Add(AddWatcherToReplicationTopology(edge, databaseWatcher, store.Urls));
            }
            await Task.WhenAll(tasks);
            foreach (var task in tasks)
            {
                resList.Add(await task);
            }
            return resList;
        }
    }
}
