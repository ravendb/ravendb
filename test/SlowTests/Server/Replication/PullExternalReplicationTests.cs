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
using Raven.Server;
using Raven.Server.Utils;
using Raven.Tests.Core.Utils.Entities;
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
                await store.Maintenance.ForDatabase(store.Database).SendAsync(new PutPullReplicationDefinitionOperation("test"));
            }
        }

        [Fact]
        public async Task PullExternalReplicationShouldWork()
        {
            var name = $"pull-replication {GetDatabaseName()}";
            using (var edge = GetDocumentStore())
            using (var central = GetDocumentStore())
            {
                await central.Maintenance.ForDatabase(central.Database).SendAsync(new PutPullReplicationDefinitionOperation(name));
                using (var s2 = central.OpenSession())
                {
                    s2.Store(new User(), "foo/bar");
                    s2.SaveChanges();
                }

                await SetupPullReplicationAsync(name, edge, central);

                var timeout = 3000;
                Assert.True(WaitForDocument(edge, "foo/bar", timeout), edge.Identifier);
            }
        }


        [Fact]
        public async Task CollectPullReplicationOngoingTaskInfo()
        {
            var name = $"pull-replication {GetDatabaseName()}";
            using (var edge = GetDocumentStore())
            using (var central = GetDocumentStore())
            {
                var centralTask = await central.Maintenance.ForDatabase(central.Database).SendAsync(new PutPullReplicationDefinitionOperation(name));
                using (var s2 = central.OpenSession())
                {
                    s2.Store(new User(), "foo/bar");
                    s2.SaveChanges();
                }

                var pullTasks = await SetupPullReplicationAsync(name, edge, central);

                var timeout = 3000;
                Assert.True(WaitForDocument(edge, "foo/bar", timeout), edge.Identifier);


                var edgeResult = (OngoingTaskPullReplicationAsEdge)await edge.Maintenance.SendAsync(new GetOngoingTaskInfoOperation(pullTasks[0].TaskId, OngoingTaskType.PullReplicationAsEdge));

                Assert.Equal(central.Database, edgeResult.DestinationDatabase);
                Assert.Equal(central.Urls[0], edgeResult.DestinationUrl);
                Assert.Equal(OngoingTaskConnectionStatus.Active, edgeResult.TaskConnectionStatus);

                var centralResult = (OngoingTaskPullReplicationAsCentral)await central.Maintenance.SendAsync(new GetOngoingTaskInfoOperation(centralTask.TaskId, OngoingTaskType.PullReplicationAsCentral));

                Assert.Equal(edge.Database, centralResult.DestinationDatabase);
                Assert.Equal(edge.Urls[0], centralResult.DestinationUrl);
                Assert.Equal(OngoingTaskConnectionStatus.Active, centralResult.TaskConnectionStatus);
            }
        }

        [Fact]
        public async Task UpdatePullReplicationOnEdge()
        {
            var definitionName1 = $"pull-replication {GetDatabaseName()}";
            var definitionName2 = $"pull-replication {GetDatabaseName()}";
            var timeout = 3000;

            using (var edge = GetDocumentStore())
            using (var central = GetDocumentStore())
            using (var central2 = GetDocumentStore())
            {
                await central.Maintenance.ForDatabase(central.Database).SendAsync(new PutPullReplicationDefinitionOperation(definitionName1));
                await central2.Maintenance.ForDatabase(central2.Database).SendAsync(new PutPullReplicationDefinitionOperation(definitionName2));

                using (var main = central.OpenSession())
                {
                    main.Store(new User(), "central1/1");
                    main.SaveChanges();
                }
                var pullTasks = await SetupPullReplicationAsync(definitionName1, edge, central);
                Assert.True(WaitForDocument(edge, "central1/1", timeout), edge.Identifier);

                
                var pull = new PullReplicationAsEdge(central2.Database, $"ConnectionString2-{edge.Database}", definitionName2)
                {
                    TaskId = pullTasks[0].TaskId
                };
                await AddWatcherToReplicationTopology(edge, pull, central2.Urls);

                using (var main = central.OpenSession())
                {
                    main.Store(new User(), "central1/2");
                    main.SaveChanges();
                }
                Assert.False(WaitForDocument(edge, "central1/2", timeout), edge.Identifier);

                using (var main = central2.OpenSession())
                {
                    main.Store(new User(), "central2");
                    main.SaveChanges();
                }
                Assert.True(WaitForDocument(edge, "central2", timeout), edge.Identifier);
            }
        }

        [Fact]
        public async Task UpdatePullReplicationOnCentral()
        {
            DebuggerAttachedTimeout.DisableLongTimespan = true;

            var definitionName = $"pull-replication {GetDatabaseName()}";
            var timeout = 3_000;

            using (var edge = GetDocumentStore())
            using (var central = GetDocumentStore())
            {
                await central.Maintenance.ForDatabase(central.Database).SendAsync(new PutPullReplicationDefinitionOperation(definitionName));

                using (var main = central.OpenSession())
                {
                    main.Store(new User(), "users/1");
                    main.SaveChanges();
                }
                await SetupPullReplicationAsync(definitionName, edge, central);
                Assert.True(WaitForDocument(edge, "users/1", timeout), edge.Identifier);

                await central.Maintenance.ForDatabase(central.Database).SendAsync(new PutPullReplicationDefinitionOperation(new PullReplicationDefinition(definitionName)
                {
                    DelayReplicationFor = TimeSpan.FromDays(1)
                }));

                using (var main = central.OpenSession())
                {
                    main.Store(new User(), "users/2");
                    main.SaveChanges();
                }
                Assert.False(WaitForDocument(edge, "users/2", timeout), edge.Identifier);

                await central.Maintenance.ForDatabase(central.Database).SendAsync(new PutPullReplicationDefinitionOperation(definitionName));
                Assert.True(WaitForDocument(edge, "users/2", timeout), edge.Identifier);
            }
        }

        [Fact]
        public async Task DisablePullReplicationOnEdge()
        {
            var definitionName = $"pull-replication {GetDatabaseName()}";
            var timeout = 3000;

            using (var edge = GetDocumentStore())
            using (var central = GetDocumentStore())
            {
                await central.Maintenance.ForDatabase(central.Database).SendAsync(new PutPullReplicationDefinitionOperation(definitionName));

                using (var main = central.OpenSession())
                {
                    main.Store(new User(), "central/1");
                    main.SaveChanges();
                }
                var pullTasks = await SetupPullReplicationAsync(definitionName, edge, central);
                Assert.True(WaitForDocument(edge, "central/1", timeout), edge.Identifier);


                var pull = new PullReplicationAsEdge(central.Database, $"ConnectionString-{edge.Database}", definitionName)
                {
                    Disabled = true,
                    TaskId = pullTasks[0].TaskId
                };
                await AddWatcherToReplicationTopology(edge, pull, central.Urls);

                using (var main = central.OpenSession())
                {
                    main.Store(new User(), "central/2");
                    main.SaveChanges();
                }
                Assert.False(WaitForDocument(edge, "central/2", timeout), edge.Identifier);

                pull.Disabled = false;
                await AddWatcherToReplicationTopology(edge, pull, central.Urls);

                using (var main = central.OpenSession())
                {
                    main.Store(new User(), "central/3");
                    main.SaveChanges();
                }
                Assert.True(WaitForDocument(edge, "central/2", timeout), edge.Identifier);
                Assert.True(WaitForDocument(edge, "central/3", timeout), edge.Identifier);
            }
        }

        [Fact]
        public async Task DisablePullReplicationOnCentral()
        {
            DebuggerAttachedTimeout.DisableLongTimespan = true;

            var definitionName = $"pull-replication {GetDatabaseName()}";
            var timeout = 3_000;

            using (var edge = GetDocumentStore())
            using (var central = GetDocumentStore())
            {
                var pullDefinition = new PullReplicationDefinition(definitionName);
                await central.Maintenance.ForDatabase(central.Database).SendAsync(new PutPullReplicationDefinitionOperation(pullDefinition));

                using (var main = central.OpenSession())
                {
                    main.Store(new User(), "users/1");
                    main.SaveChanges();
                }
                await SetupPullReplicationAsync(definitionName, edge, central);
                Assert.True(WaitForDocument(edge, "users/1", timeout), edge.Identifier);

                pullDefinition.Disabled = true;
                await central.Maintenance.ForDatabase(central.Database).SendAsync(new PutPullReplicationDefinitionOperation(pullDefinition));

                using (var main = central.OpenSession())
                {
                    main.Store(new User(), "users/2");
                    main.SaveChanges();
                }
                Assert.False(WaitForDocument(edge, "users/2", timeout), edge.Identifier);

                pullDefinition.Disabled = false;
                await central.Maintenance.ForDatabase(central.Database).SendAsync(new PutPullReplicationDefinitionOperation(pullDefinition));

                Assert.True(WaitForDocument(edge, "users/2", timeout), edge.Identifier);
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
                await central.Maintenance.ForDatabase(central.Database).SendAsync(new PutPullReplicationDefinitionOperation(name));
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
                await SetupPullReplicationAsync(pullReplicationName, edgeStore, pullReplicationCertificate, centralStore);
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
                await store.Maintenance.ForDatabase(store.Database).SendAsync(new PutPullReplicationDefinitionOperation(new PullReplicationDefinition(pullReplicationName)
                {
                    Certificates = new Dictionary<string, string>
                    {
                        [certificate.Thumbprint] = Convert.ToBase64String(certificate.Export(X509ContentType.Cert))
                    }
                }));
            }
        }

        [Fact]
        public async Task FailoverOnCentralNodeFail()
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
                await centralStore.Maintenance.ForDatabase(centralStore.Database).SendAsync(new PutPullReplicationDefinitionOperation(name));

                // add pull replication with invalid discovery url to test the failover on database topology discovery
                var pullReplication = new PullReplicationAsEdge(centralDB, $"ConnectionString-{centralDB}", name)
                {
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
                    () => handler.GetOngoingTasksInternal().OngoingTasksList.Single(t => t is OngoingTaskPullReplicationAsEdge).As<OngoingTaskPullReplicationAsEdge>().DestinationUrl !=
                          null,
                    true));

                var watcherTaskUrl = handler.GetOngoingTasksInternal().OngoingTasksList.Single(t => t is OngoingTaskPullReplicationAsEdge).As<OngoingTaskPullReplicationAsEdge>()
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

                WaitForUserToContinueTheTest(minionStore);

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
        public async Task FailoverOnEdgeNodeFail()
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
                await centralStore.Maintenance.ForDatabase(centralStore.Database).SendAsync(new PutPullReplicationDefinitionOperation(name));


                // add pull replication with invalid discovery url to test the failover on database topology discovery
                var pullReplication = new PullReplicationAsEdge(centralDB, $"ConnectionString-{centralDB}",name)
                {
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
                    () => handler.GetOngoingTasksInternal().OngoingTasksList.Single(t => t is OngoingTaskPullReplicationAsEdge).As<OngoingTaskPullReplicationAsEdge>().DestinationUrl !=
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

        public async Task<List<ModifyOngoingTaskResult>> SetupPullReplicationAsync(string remoteName, DocumentStore edge, X509Certificate2 certificate, params DocumentStore[] central)
        {
            var tasks = new List<Task<ModifyOngoingTaskResult>>();
            var resList = new List<ModifyOngoingTaskResult>();
            foreach (var store in central)
            {
                var pull = new PullReplicationAsEdge(store.Database,$"ConnectionString-{store.Database}",remoteName);
                if (certificate != null)
                {
                    pull.CertificateWithPrivateKey = Convert.ToBase64String(certificate.Export(X509ContentType.Pfx));
                }
                ModifyReplicationDestination(pull);
                tasks.Add(AddWatcherToReplicationTopology(edge, pull, store.Urls));
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
