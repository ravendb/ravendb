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
    public class PullReplicationTests : ReplicationTestBase
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
        public async Task PullReplicationShouldWork()
        {
            var name = $"pull-replication {GetDatabaseName()}";
            using (var sink = GetDocumentStore())
            using (var hub = GetDocumentStore())
            {
                await hub.Maintenance.ForDatabase(hub.Database).SendAsync(new PutPullReplicationDefinitionOperation(name));
                using (var s2 = hub.OpenSession())
                {
                    s2.Store(new User(), "foo/bar");
                    s2.SaveChanges();
                }

                await SetupPullReplicationAsync(name, sink, hub);

                var timeout = 3000;
                Assert.True(WaitForDocument(sink, "foo/bar", timeout), sink.Identifier);
            }
        }


        [Fact]
        public async Task CollectPullReplicationOngoingTaskInfo()
        {
            var name = $"pull-replication {GetDatabaseName()}";
            using (var sink = GetDocumentStore())
            using (var hub = GetDocumentStore())
            {
                var hubTask = await hub.Maintenance.ForDatabase(hub.Database).SendAsync(new PutPullReplicationDefinitionOperation(name));
                using (var s2 = hub.OpenSession())
                {
                    s2.Store(new User(), "foo/bar");
                    s2.SaveChanges();
                }

                var pullTasks = await SetupPullReplicationAsync(name, sink, hub);

                var timeout = 3000;
                Assert.True(WaitForDocument(sink, "foo/bar", timeout), sink.Identifier);


                var sinkResult = (OngoingTaskPullReplicationAsSink)await sink.Maintenance.SendAsync(new GetOngoingTaskInfoOperation(pullTasks[0].TaskId, OngoingTaskType.PullReplicationAsSink));

                Assert.Equal(hub.Database, sinkResult.DestinationDatabase);
                Assert.Equal(hub.Urls[0], sinkResult.DestinationUrl);
                Assert.Equal(OngoingTaskConnectionStatus.Active, sinkResult.TaskConnectionStatus);

                var hubResult = (OngoingTaskPullReplicationAsHub)await hub.Maintenance.SendAsync(new GetOngoingTaskInfoOperation(hubTask.TaskId, OngoingTaskType.PullReplicationAsHub));

                Assert.Equal(sink.Database, hubResult.DestinationDatabase);
                Assert.Equal(sink.Urls[0], hubResult.DestinationUrl);
                Assert.Equal(OngoingTaskConnectionStatus.Active, hubResult.TaskConnectionStatus);
            }
        }

        [Fact]
        public async Task UpdatePullReplicationOnSink()
        {
            var definitionName1 = $"pull-replication {GetDatabaseName()}";
            var definitionName2 = $"pull-replication {GetDatabaseName()}";
            var timeout = 3000;

            using (var sink = GetDocumentStore())
            using (var hub = GetDocumentStore())
            using (var hub2 = GetDocumentStore())
            {
                await hub.Maintenance.ForDatabase(hub.Database).SendAsync(new PutPullReplicationDefinitionOperation(definitionName1));
                await hub2.Maintenance.ForDatabase(hub2.Database).SendAsync(new PutPullReplicationDefinitionOperation(definitionName2));

                using (var main = hub.OpenSession())
                {
                    main.Store(new User(), "hub1/1");
                    main.SaveChanges();
                }
                var pullTasks = await SetupPullReplicationAsync(definitionName1, sink, hub);
                Assert.True(WaitForDocument(sink, "hub1/1", timeout), sink.Identifier);

                
                var pull = new PullReplicationAsSink(hub2.Database, $"ConnectionString2-{sink.Database}", definitionName2)
                {
                    TaskId = pullTasks[0].TaskId
                };
                await AddWatcherToReplicationTopology(sink, pull, hub2.Urls);

                using (var main = hub.OpenSession())
                {
                    main.Store(new User(), "hub1/2");
                    main.SaveChanges();
                }
                Assert.False(WaitForDocument(sink, "hub1/2", timeout), sink.Identifier);

                using (var main = hub2.OpenSession())
                {
                    main.Store(new User(), "hub2");
                    main.SaveChanges();
                }
                Assert.True(WaitForDocument(sink, "hub2", timeout), sink.Identifier);
            }
        }

        [Fact]
        public async Task UpdatePullReplicationOnHub()
        {
            DebuggerAttachedTimeout.DisableLongTimespan = true;

            var definitionName = $"pull-replication {GetDatabaseName()}";
            var timeout = 3_000;

            using (var sink = GetDocumentStore())
            using (var hub = GetDocumentStore())
            {
                await hub.Maintenance.ForDatabase(hub.Database).SendAsync(new PutPullReplicationDefinitionOperation(definitionName));

                using (var main = hub.OpenSession())
                {
                    main.Store(new User(), "users/1");
                    main.SaveChanges();
                }
                await SetupPullReplicationAsync(definitionName, sink, hub);
                Assert.True(WaitForDocument(sink, "users/1", timeout), sink.Identifier);

                await hub.Maintenance.ForDatabase(hub.Database).SendAsync(new PutPullReplicationDefinitionOperation(new PullReplicationDefinition(definitionName)
                {
                    DelayReplicationFor = TimeSpan.FromDays(1)
                }));

                using (var main = hub.OpenSession())
                {
                    main.Store(new User(), "users/2");
                    main.SaveChanges();
                }
                Assert.False(WaitForDocument(sink, "users/2", timeout), sink.Identifier);

                await hub.Maintenance.ForDatabase(hub.Database).SendAsync(new PutPullReplicationDefinitionOperation(definitionName));
                Assert.True(WaitForDocument(sink, "users/2", timeout), sink.Identifier);
            }
        }

        [Fact]
        public async Task DisablePullReplicationOnSink()
        {
            var definitionName = $"pull-replication {GetDatabaseName()}";
            var timeout = 3000;

            using (var sink = GetDocumentStore())
            using (var hub = GetDocumentStore())
            {
                await hub.Maintenance.ForDatabase(hub.Database).SendAsync(new PutPullReplicationDefinitionOperation(definitionName));

                using (var main = hub.OpenSession())
                {
                    main.Store(new User(), "hub/1");
                    main.SaveChanges();
                }
                var pullTasks = await SetupPullReplicationAsync(definitionName, sink, hub);
                Assert.True(WaitForDocument(sink, "hub/1", timeout), sink.Identifier);


                var pull = new PullReplicationAsSink(hub.Database, $"ConnectionString-{sink.Database}", definitionName)
                {
                    Disabled = true,
                    TaskId = pullTasks[0].TaskId
                };
                await AddWatcherToReplicationTopology(sink, pull, hub.Urls);

                using (var main = hub.OpenSession())
                {
                    main.Store(new User(), "hub/2");
                    main.SaveChanges();
                }
                Assert.False(WaitForDocument(sink, "hub/2", timeout), sink.Identifier);

                pull.Disabled = false;
                await AddWatcherToReplicationTopology(sink, pull, hub.Urls);

                using (var main = hub.OpenSession())
                {
                    main.Store(new User(), "hub/3");
                    main.SaveChanges();
                }
                Assert.True(WaitForDocument(sink, "hub/2", timeout), sink.Identifier);
                Assert.True(WaitForDocument(sink, "hub/3", timeout), sink.Identifier);
            }
        }

        [Fact]
        public async Task DisablePullReplicationOnHub()
        {
            DebuggerAttachedTimeout.DisableLongTimespan = true;

            var definitionName = $"pull-replication {GetDatabaseName()}";
            var timeout = 3_000;

            using (var sink = GetDocumentStore())
            using (var hub = GetDocumentStore())
            {
                var pullDefinition = new PullReplicationDefinition(definitionName);
                await hub.Maintenance.ForDatabase(hub.Database).SendAsync(new PutPullReplicationDefinitionOperation(pullDefinition));

                using (var main = hub.OpenSession())
                {
                    main.Store(new User(), "users/1");
                    main.SaveChanges();
                }
                await SetupPullReplicationAsync(definitionName, sink, hub);
                Assert.True(WaitForDocument(sink, "users/1", timeout), sink.Identifier);

                pullDefinition.Disabled = true;
                await hub.Maintenance.ForDatabase(hub.Database).SendAsync(new PutPullReplicationDefinitionOperation(pullDefinition));

                using (var main = hub.OpenSession())
                {
                    main.Store(new User(), "users/2");
                    main.SaveChanges();
                }
                Assert.False(WaitForDocument(sink, "users/2", timeout), sink.Identifier);

                pullDefinition.Disabled = false;
                await hub.Maintenance.ForDatabase(hub.Database).SendAsync(new PutPullReplicationDefinitionOperation(pullDefinition));

                Assert.True(WaitForDocument(sink, "users/2", timeout), sink.Identifier);
            }
        }

        [Fact]
        public async Task MultiplePullExternalReplicationShouldWork()
        {
            var name = $"pull-replication {GetDatabaseName()}";
            using (var hub = GetDocumentStore())
            using (var sink1 = GetDocumentStore())
            using (var sink2 = GetDocumentStore())
            {
                await hub.Maintenance.ForDatabase(hub.Database).SendAsync(new PutPullReplicationDefinitionOperation(name));
                using (var session = hub.OpenSession())
                {
                    session.Store(new User(), "foo/bar");
                    session.SaveChanges();
                }

                await SetupPullReplicationAsync(name, sink1, hub);
                await SetupPullReplicationAsync(name, sink2, hub);

                var timeout = 3000;
                Assert.True(WaitForDocument(sink1, "foo/bar", timeout), sink1.Identifier);
                Assert.True(WaitForDocument(sink2, "foo/bar", timeout), sink2.Identifier);
            }
        }

        [Fact]
        public async Task PullExternalReplicationWithCertificateShouldWork()
        {
            var hubSettings = new ConcurrentDictionary<string, string>();
            var sinkSettings = new ConcurrentDictionary<string, string>();

            var hubCertPath = SetupServerAuthentication(hubSettings, createNew: true);
            var sinkCertPath = SetupServerAuthentication(sinkSettings, createNew: true);

            var hubDB = GetDatabaseName();
            var sinkDB = GetDatabaseName();
            var pullReplicationName = $"{hubDB}-pull";

            var hubServer = GetNewServer(hubSettings);
            var sinkServer = GetNewServer(sinkSettings);

            var hubAdminCert = new X509Certificate2(hubCertPath, (string)null, X509KeyStorageFlags.MachineKeySet);
            var sinkAdminCert = new X509Certificate2(sinkCertPath, (string)null, X509KeyStorageFlags.MachineKeySet);

            var dummy = GenerateAndSaveSelfSignedCertificate(true);
            var pullReplicationCertificate = new X509Certificate2(dummy, (string)null, X509KeyStorageFlags.MachineKeySet | X509KeyStorageFlags.Exportable);
            Assert.True(pullReplicationCertificate.HasPrivateKey);

            await PutCertificateInhub(pullReplicationName, hubServer, hubAdminCert, hubDB, pullReplicationCertificate);

            using (var hubStore = GetDocumentStore(new Options
            {
                ClientCertificate = hubAdminCert,
                Server = hubServer,
                CreateDatabase = false,
                ModifyDatabaseName = _ => hubDB
            }))
            using (var sinkStore = GetDocumentStore(new Options
            {
                ClientCertificate = sinkAdminCert,
                Server = sinkServer,
                ModifyDatabaseName = _ => sinkDB
            }))
            {
                await SetupPullReplicationAsync(pullReplicationName, sinkStore, pullReplicationCertificate, hubStore);
                using (var hubSession = hubStore.OpenSession())
                {
                    hubSession.Store(new User(), "foo/bar");
                    hubSession.SaveChanges();
                }

                var timeout = 3000;
                Assert.True(WaitForDocument(sinkStore, "foo/bar", timeout), sinkStore.Identifier);
            }
        }

        private async Task PutCertificateInhub(string pullReplicationName, RavenServer server, X509Certificate2 hubAdminCert, string hubDB,
            X509Certificate2 certificate)
        {
            using (var store = GetDocumentStore(new Options
            {
                ClientCertificate = hubAdminCert,
                Server = server,
                ModifyDatabaseName = _=> hubDB
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
        public async Task FailoverOnHubNodeFail()
        {
            var clusterSize = 3;
            var hub = await CreateRaftClusterAndGetLeader(clusterSize);
            var minion = await CreateRaftClusterAndGetLeader(clusterSize);

            var hubDB = GetDatabaseName();
            var minionDB = GetDatabaseName();

            var dstTopology = await CreateDatabaseInCluster(minionDB, clusterSize, minion.WebUrl);
            var srcTopology = await CreateDatabaseInCluster(hubDB, clusterSize, hub.WebUrl);

            using (var hubStore = new DocumentStore
            {
                Urls = new[] { hub.WebUrl },
                Database = hubDB
            }.Initialize())
            using (var minionStore = new DocumentStore
            {
                Urls = new[] { minion.WebUrl },
                Database = minionDB
            }.Initialize())
            {
                using (var session = hubStore.OpenSession())
                {
                    session.Advanced.WaitForReplicationAfterSaveChanges(timeout: TimeSpan.FromSeconds(10), replicas: clusterSize - 1);
                    session.Store(new User
                    {
                        Name = "Karmel"
                    }, "users/1");
                    session.SaveChanges();
                }

                var name = $"pull-replication {GetDatabaseName()}";
                await hubStore.Maintenance.ForDatabase(hubStore.Database).SendAsync(new PutPullReplicationDefinitionOperation(name));

                // add pull replication with invalid discovery url to test the failover on database topology discovery
                var pullReplication = new PullReplicationAsSink(hubDB, $"ConnectionString-{hubDB}", name)
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
                    () => handler.GetOngoingTasksInternal().OngoingTasksList.Single(t => t is OngoingTaskPullReplicationAsSink).As<OngoingTaskPullReplicationAsSink>().DestinationUrl !=
                          null,
                    true));

                var watcherTaskUrl = handler.GetOngoingTasksInternal().OngoingTasksList.Single(t => t is OngoingTaskPullReplicationAsSink).As<OngoingTaskPullReplicationAsSink>()
                    .DestinationUrl;

                // dispose the hub node, from which we are currently pulling 
                DisposeServerAndWaitForFinishOfDisposal(Servers.Single(s => s.WebUrl == watcherTaskUrl));

                using (var session = hubStore.OpenSession())
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
        public async Task FailoverOnSinkNodeFail()
        {
            var clusterSize = 3;
            var hub = await CreateRaftClusterAndGetLeader(clusterSize);
            var minion = await CreateRaftClusterAndGetLeader(clusterSize);

            var hubDB = GetDatabaseName();
            var minionDB = GetDatabaseName();

            var dstTopology = await CreateDatabaseInCluster(minionDB, clusterSize, minion.WebUrl);
            var srcTopology = await CreateDatabaseInCluster(hubDB, clusterSize, hub.WebUrl);

            using (var hubStore = new DocumentStore
            {
                Urls = new[] { hub.WebUrl },
                Database = hubDB
            }.Initialize())
            using (var minionStore = new DocumentStore
            {
                Urls = new[] { minion.WebUrl },
                Database = minionDB
            }.Initialize())
            {
                using (var session = hubStore.OpenSession())
                {
                    session.Advanced.WaitForReplicationAfterSaveChanges(timeout: TimeSpan.FromSeconds(10), replicas: clusterSize - 1);
                    session.Store(new User
                    {
                        Name = "Karmel"
                    }, "users/1");
                    session.SaveChanges();
                }

                var name = $"pull-replication {GetDatabaseName()}";
                await hubStore.Maintenance.ForDatabase(hubStore.Database).SendAsync(new PutPullReplicationDefinitionOperation(name));


                // add pull replication with invalid discovery url to test the failover on database topology discovery
                var pullReplication = new PullReplicationAsSink(hubDB, $"ConnectionString-{hubDB}",name)
                {
                    MentorNode = "B", // this is the node were the data will be replicated to.
                };
                await AddWatcherToReplicationTopology((DocumentStore)minionStore, pullReplication, new[] { "http://127.0.0.1:1234", hub.WebUrl });

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
                    () => handler.GetOngoingTasksInternal().OngoingTasksList.Single(t => t is OngoingTaskPullReplicationAsSink).As<OngoingTaskPullReplicationAsSink>().DestinationUrl !=
                          null,
                    true));

                // dispose the minion node.
                DisposeServerAndWaitForFinishOfDisposal(server);

                using (var session = hubStore.OpenSession())
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

        public Task<List<ModifyOngoingTaskResult>> SetupPullReplicationAsync(string remoteName, DocumentStore sink, params DocumentStore[] hub)
        {
            return SetupPullReplicationAsync(remoteName, sink, null, hub);
        }

        public async Task<List<ModifyOngoingTaskResult>> SetupPullReplicationAsync(string remoteName, DocumentStore sink, X509Certificate2 certificate, params DocumentStore[] hub)
        {
            var tasks = new List<Task<ModifyOngoingTaskResult>>();
            var resList = new List<ModifyOngoingTaskResult>();
            foreach (var store in hub)
            {
                var pull = new PullReplicationAsSink(store.Database,$"ConnectionString-{store.Database}",remoteName);
                if (certificate != null)
                {
                    pull.CertificateWithPrivateKey = Convert.ToBase64String(certificate.Export(X509ContentType.Pfx));
                }
                ModifyReplicationDestination(pull);
                tasks.Add(AddWatcherToReplicationTopology(sink, pull, store.Urls));
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
