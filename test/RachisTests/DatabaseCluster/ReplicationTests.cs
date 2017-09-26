using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using Esprima.Ast;
using FastTests.Server.Replication;
using Raven.Client.Documents;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions.Security;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Server.Documents.Replication;
using Raven.Server.Utils;
using Raven.Server.Web.System;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;

namespace RachisTests.DatabaseCluster
{
    public class ReplicationTests : ReplicationTestBase
    {

        [NightlyBuildFact]
        public async Task WaitForCommandToApply()
        {
            var clusterSize = 5;
            var databaseName = GetDatabaseName();
            var leader = await CreateRaftClusterAndGetLeader(clusterSize, false, 0);
            using (var store = new DocumentStore
            {
                Urls = new[] {leader.WebUrl},
                Database = databaseName
            }.Initialize())
            {
                var doc = new DatabaseRecord(databaseName);
                doc.Topology = new DatabaseTopology
                {
                    Members = new List<string>
                    {
                        "B"
                    }
                };
                var res = await store.Admin.Server.SendAsync(new CreateDatabaseOperation(doc));
                Assert.NotEqual(res.Topology.Members.First(), leader.ServerStore.NodeTag);

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User());
                    await session.SaveChangesAsync();
                }
            }
        }

        [NightlyBuildTheory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task EnsureDocumentsReplication(bool useSsl)
        {
            var clusterSize = 5;
            var databaseName = GetDatabaseName();
            var leader = await CreateRaftClusterAndGetLeader(clusterSize, false, useSsl: useSsl);

            X509Certificate2 clientCertificate = null;
            X509Certificate2 adminCertificate = null;
            if (useSsl)
            {

                adminCertificate = AskServerForClientCertificate(_selfSignedCertFileName, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin, server: leader);
                clientCertificate = AskServerForClientCertificate(_selfSignedCertFileName, new Dictionary<string, DatabaseAccess>
                {
                    [databaseName] = DatabaseAccess.Admin
                }, server: leader);
            }

            DatabasePutResult databaseResult;
            using (var store = new DocumentStore
            {
                Urls = new[] {leader.WebUrl},
                Database = databaseName,
                Certificate = adminCertificate,
                Conventions =
                {
                    DisableTopologyUpdates = true
                }
            }.Initialize())
            {
                var doc = new DatabaseRecord(databaseName);
                databaseResult = await store.Admin.Server.SendAsync(new CreateDatabaseOperation(doc, clusterSize));
            }
            Assert.Equal(clusterSize, databaseResult.Topology.AllNodes.Count());
            foreach (var server in Servers)
            {
                await server.ServerStore.Cluster.WaitForIndexNotification(databaseResult.RaftCommandIndex);
            }
            foreach (var server in Servers.Where(s => databaseResult.NodesAddedTo.Any(n => n == s.WebUrl)))
            {
                await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(databaseName);
            }

            using (var store = new DocumentStore()
            {
                Urls = new[] { databaseResult.NodesAddedTo[0] },
                Database = databaseName,
                Certificate = clientCertificate,
                Conventions =
                {
                    DisableTopologyUpdates = true
                }
            }.Initialize())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Karmel" }, "users/1");
                    await session.SaveChangesAsync();
                }
                Assert.True(await WaitForDocumentInClusterAsync<User>(
                    databaseResult.Topology,
                    databaseName,
                    "users/1",
                    u => u.Name.Equals("Karmel"),
                    TimeSpan.FromSeconds(clusterSize + 5),
                    certificate: clientCertificate));

            }
        }

        [NightlyBuildTheory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task EnsureReplicationToWatchers(bool useSsl)
        {
            var clusterSize = 3;
            var databaseName = GetDatabaseName();
            var leader = await CreateRaftClusterAndGetLeader(clusterSize, useSsl: useSsl);
            var watchers = new List<ExternalReplication>();

            X509Certificate2 adminCertificate = null;
            if (useSsl)
            {
                adminCertificate = AskServerForClientCertificate(_selfSignedCertFileName, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin, server: leader);
            }


            using (var store = new DocumentStore()
            {
                Urls = new[] {leader.WebUrl},
                Database = databaseName,
                Certificate = adminCertificate,
                Conventions =
                {
                    DisableTopologyUpdates = true
                }
            }.Initialize())
            {
                var doc = new DatabaseRecord(databaseName);
                var databaseResult = await store.Admin.Server.SendAsync(new CreateDatabaseOperation(doc, clusterSize));
                Assert.Equal(clusterSize, databaseResult.Topology.AllNodes.Count());
                foreach (var server in Servers)
                {
                    await server.ServerStore.Cluster.WaitForIndexNotification(databaseResult.RaftCommandIndex);
                }
                foreach (var server in Servers)
                {
                    await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(databaseName);
                }
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Karmel" }, "users/1");
                    await session.SaveChangesAsync();
                }
                Assert.True(await WaitForDocumentInClusterAsync<User>(
                    databaseResult.Topology,
                    databaseName,
                    "users/1",
                    u => u.Name.Equals("Karmel"),
                    TimeSpan.FromSeconds(clusterSize + 5),
                    adminCertificate));

                for (var i = 0; i < 5; i++)
                {
                    doc = new DatabaseRecord($"Watcher{i}");
                    var res = await store.Admin.Server.SendAsync(new CreateDatabaseOperation(doc));
                    var server = Servers.Single(x => x.WebUrl == res.NodesAddedTo[0]);
                    await server.ServerStore.Cluster.WaitForIndexNotification(res.RaftCommandIndex);
                    await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore($"Watcher{i}");

                    var watcher = new ExternalReplication(res.NodesAddedTo.ToArray())
                    {
                        Database = $"Watcher{i}",
                    };
                    watchers.Add(watcher);

                    await AddWatcherToReplicationTopology((DocumentStore)store, watcher);

                }
            }

            foreach (var watcher in watchers)
            {
                using (var store = new DocumentStore
                {
                    Urls = new[] { watcher.Url },
                    Database = watcher.Database,
                    Certificate = adminCertificate,
                    Conventions =
                    {
                        DisableTopologyUpdates = true
                    }
                }.Initialize())
                {
                    Assert.True(WaitForDocument<User>(store, "users/1", u => u.Name == "Karmel"));
                }
            }
        }

        [NightlyBuildFact]
        public async Task SetMetorToExternalReplication()
        {
            var clusterSize = 5;
            var databaseName = GetDatabaseName();
            var leader = await CreateRaftClusterAndGetLeader(clusterSize);
            var watchers = new List<ExternalReplication>();

            using (var store = new DocumentStore()
            {
                Urls = new[] { leader.WebUrl },
                Database = databaseName,
            }.Initialize())
            {
                var doc = new DatabaseRecord(databaseName);
                var databaseResult = await store.Admin.Server.SendAsync(new CreateDatabaseOperation(doc, clusterSize));
                Assert.Equal(clusterSize, databaseResult.Topology.AllNodes.Count());
                foreach (var server in Servers)
                {
                    await server.ServerStore.Cluster.WaitForIndexNotification(databaseResult.RaftCommandIndex);
                }
                foreach (var server in Servers)
                {
                    await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(databaseName);
                }
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Karmel" }, "users/1");
                    await session.SaveChangesAsync();
                }
                Assert.True(await WaitForDocumentInClusterAsync<User>(
                    databaseResult.Topology,
                    databaseName,
                    "users/1",
                    u => u.Name.Equals("Karmel"),
                    TimeSpan.FromSeconds(clusterSize + 5)));

                for (var i = 0; i < 5; i++)
                {
                    doc = new DatabaseRecord($"Watcher{i}");
                    var res = await store.Admin.Server.SendAsync(new CreateDatabaseOperation(doc));
                    var server = Servers.Single(x => x.WebUrl == res.NodesAddedTo[0]);
                    await server.ServerStore.Cluster.WaitForIndexNotification(res.RaftCommandIndex);
                    await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore($"Watcher{i}");

                    var watcher = new ExternalReplication
                    {
                        Database = $"Watcher{i}",
                        Url = res.NodesAddedTo[0],
                        MentorNode = "C"
                    };
                    watchers.Add(watcher);

                    var taskRes = await AddWatcherToReplicationTopology((DocumentStore)store, watcher);
                    var replicationResult = (OngoingTaskReplication)await GetTaskInfo((DocumentStore)store, taskRes.TaskId, OngoingTaskType.Replication);
                    Assert.Equal("C",replicationResult.ResponsibleNode.NodeTag);
                }
            }

            foreach (var watcher in watchers)
            {
                using (var store = new DocumentStore
                {
                    Urls = new[] { watcher.Url },
                    Database = watcher.Database,
                }.Initialize())
                {
                    Assert.True(WaitForDocument<User>(store, "users/1", u => u.Name == "Karmel"));
                }
            }
        }


        [NightlyBuildFact]
        public async Task CanAddAndModifySingleWatcher()
        {
            var clusterSize = 3;
            var databaseName = "ReplicationTestDB";
            var leader = await CreateRaftClusterAndGetLeader(clusterSize);
            ExternalReplication watcher;

            using (var store = new DocumentStore()
            {
                Urls = new[] {leader.WebUrl},
                Database = databaseName,
                Conventions =
                {
                    DisableTopologyUpdates = true
                }
            }.Initialize())
            {
                var doc = new DatabaseRecord(databaseName);
                var databaseResult = await store.Admin.Server.SendAsync(new CreateDatabaseOperation(doc, clusterSize));
                Assert.Equal(clusterSize, databaseResult.Topology.AllNodes.Count());
                foreach (var server in Servers)
                {
                    await server.ServerStore.Cluster.WaitForIndexNotification(databaseResult.RaftCommandIndex);
                }
                foreach (var server in Servers)
                {
                    await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(databaseName);
                }
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Karmel" }, "users/1");
                    await session.SaveChangesAsync();
                }
                Assert.True(await WaitForDocumentInClusterAsync<User>(
                    databaseResult.Topology,
                    databaseName,
                    "users/1",
                    u => u.Name.Equals("Karmel"),
                    TimeSpan.FromSeconds(clusterSize + 5)));


                doc = new DatabaseRecord("Watcher");
                var res = await store.Admin.Server.SendAsync(new CreateDatabaseOperation(doc));
                var node = Servers.Single(x => x.WebUrl == res.NodesAddedTo[0]);
                await node.ServerStore.Cluster.WaitForIndexNotification(res.RaftCommandIndex);
                await node.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore("Watcher");

                watcher = new ExternalReplication(res.NodesAddedTo.ToArray())
                {
                    Database = "Watcher",
                    Name = "MyExternalReplication1"
                };

                await AddWatcherToReplicationTopology((DocumentStore)store, watcher);
            }

            var tasks = OngoingTasksHandler.GetOngoingTasksFor(databaseName, leader.ServerStore);
            Assert.Equal(1, tasks.OngoingTasksList.Count);
            var repTask = tasks.OngoingTasksList[0] as OngoingTaskReplication;
            Assert.Equal(repTask?.DestinationDatabase, watcher.Database);
            Assert.Equal(repTask?.DestinationUrl, watcher.Url);
            Assert.Equal(repTask?.TaskName, watcher.Name);

            watcher.TaskId = Convert.ToInt64(repTask?.TaskId);

            using (var store = new DocumentStore
            {
                Urls = new[] { watcher.Url },
                Database = watcher.Database,
                Conventions =
                {
                    DisableTopologyUpdates = true
                }
            }.Initialize())
            {
                Assert.True(WaitForDocument<User>(store, "users/1", u => u.Name == "Karmel"));
            }

            using (var store = new DocumentStore
            {
                Urls = new[] {leader.WebUrl},
                Database = databaseName,
                Conventions =
                {
                    DisableTopologyUpdates = true
                }
            }.Initialize())
            {
                var doc = new DatabaseRecord("Watcher2");
                var res = await store.Admin.Server.SendAsync(new CreateDatabaseOperation(doc));
                var node = Servers.Single(x => x.WebUrl == res.NodesAddedTo[0]);
                await node.ServerStore.Cluster.WaitForIndexNotification(res.RaftCommandIndex);
                await node.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore("Watcher2");

                //modify watcher
                watcher.Database = "Watcher2";
                watcher.Url = res.NodesAddedTo[0];
                watcher.Name = "MyExternalReplication2";

                await AddWatcherToReplicationTopology((DocumentStore)store, watcher);
            }

            tasks = OngoingTasksHandler.GetOngoingTasksFor(databaseName, leader.ServerStore);
            Assert.Equal(1, tasks.OngoingTasksList.Count);
            repTask = tasks.OngoingTasksList[0] as OngoingTaskReplication;
            Assert.Equal(repTask?.DestinationDatabase, watcher.Database);
            Assert.Equal(repTask?.DestinationUrl, watcher.Url);
            Assert.Equal(repTask?.TaskName, watcher.Name);

            using (var store = new DocumentStore
            {
                Urls = new[] { watcher.Url },
                Database = watcher.Database,
                Conventions =
                {
                    DisableTopologyUpdates = true
                }
            }.Initialize())
            {
                Assert.True(WaitForDocument<User>(store, "users/1", u => u.Name == "Karmel"));
            }

            //delete watcher
            using (var store = new DocumentStore
            {
                Urls = new[] {leader.WebUrl},
                Database = databaseName,
                Conventions =
                {
                    DisableTopologyUpdates = true
                }
            }.Initialize())
            {
                await DeleteOngoingTask((DocumentStore)store, watcher.TaskId, OngoingTaskType.Replication);
                tasks = OngoingTasksHandler.GetOngoingTasksFor(databaseName, leader.ServerStore);
                Assert.Equal(0, tasks.OngoingTasksList.Count);
            }
        }


        [NightlyBuildTheory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task DoNotReplicateBack(bool useSsl)
        {
            var clusterSize = 5;
            var databaseName = GetDatabaseName();
            var leader = await CreateRaftClusterAndGetLeader(clusterSize, useSsl: useSsl);

            X509Certificate2 adminCertificate = null;
            if (useSsl)
            {
                adminCertificate = AskServerForClientCertificate(_selfSignedCertFileName, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin, server: leader);
            }

            using (var store = new DocumentStore()
            {
                Urls = new[] {leader.WebUrl},
                Database = databaseName,
                Certificate = adminCertificate,
                Conventions =
                {
                    DisableTopologyUpdates = true
                }
            }.Initialize())
            {
                var doc = new DatabaseRecord(databaseName);
                var databaseResult = await store.Admin.Server.SendAsync(new CreateDatabaseOperation(doc, clusterSize));
                var topology = databaseResult.Topology;
                Assert.Equal(clusterSize, topology.AllNodes.Count());

                await WaitForValueOnGroupAsync(topology, s =>
               {
                   var db = s.DatabasesLandlord.TryGetOrCreateResourceStore(databaseName).Result;
                   return db.ReplicationLoader?.OutgoingConnections.Count();
               }, clusterSize - 1);

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Karmel" }, "users/1");
                    await session.SaveChangesAsync();
                }
                Assert.True(await WaitForDocumentInClusterAsync<User>(
                    databaseResult.Topology,
                    databaseName,
                    "users/1",
                    u => u.Name.Equals("Karmel"),
                    TimeSpan.FromSeconds(clusterSize + 5),
                    certificate: adminCertificate));

                topology.RemoveFromTopology(leader.ServerStore.NodeTag);
                await Task.Delay(200); // twice the heartbeat
                await Assert.ThrowsAsync<Exception>(async () =>
                {
                    await WaitForValueOnGroupAsync(topology, (s) =>
                    {
                        var db = s.DatabasesLandlord.TryGetOrCreateResourceStore(databaseName).Result;
                        return db.ReplicationLoader?.OutgoingHandlers.Any(o => o.GetReplicationPerformance().Any(p => p.Network.DocumentOutputCount > 0)) ?? false;
                    }, true);
                });
            }
        }

        [NightlyBuildTheory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task AddGlobalChangeVectorToNewDocument(bool useSsl)
        {
            var clusterSize = 3;
            var databaseName = GetDatabaseName();
            var leader = await CreateRaftClusterAndGetLeader(clusterSize, true, 0, useSsl: useSsl);

            X509Certificate2 clientCertificate = null;
            X509Certificate2 adminCertificate = null;
            if (useSsl)
            {

                adminCertificate = AskServerForClientCertificate(_selfSignedCertFileName, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin, server: leader);
                clientCertificate = AskServerForClientCertificate(_selfSignedCertFileName, new Dictionary<string, DatabaseAccess>
                {
                    [databaseName] = DatabaseAccess.Admin
                }, server: leader);
            }
            DatabaseTopology topology;
            var doc = new DatabaseRecord(databaseName);
            using (var store = new DocumentStore()
            {
                Urls = new[] {leader.WebUrl},
                Database = databaseName,
                Certificate = adminCertificate,
                Conventions =
                {
                    DisableTopologyUpdates = true
                }
            }.Initialize())
            {
                var databaseResult = await store.Admin.Server.SendAsync(new CreateDatabaseOperation(doc, clusterSize));
                topology = databaseResult.Topology;
                Assert.Equal(clusterSize, topology.AllNodes.Count());
                foreach (var server in Servers)
                {
                    await server.ServerStore.Cluster.WaitForIndexNotification(databaseResult.RaftCommandIndex);
                }
                foreach (var server in Servers)
                {
                    await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(databaseName);
                }
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Karmel" }, "users/1");
                    await session.SaveChangesAsync();
                }
                Assert.True(await WaitForDocumentInClusterAsync<User>(
                    topology,
                    databaseName,
                    "users/1",
                    u => u.Name.Equals("Karmel"),
                    TimeSpan.FromSeconds(clusterSize + 5),
                    certificate: adminCertificate));
            }

            using (var store = new DocumentStore()
            {
                Urls = new[] {Servers[1].WebUrl},
                Database = databaseName,
                Certificate = clientCertificate,
                Conventions =
                {
                    DisableTopologyUpdates = true
                }
            }.Initialize())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Indych" }, "users/2");
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>("users/2");
                    var changeVector = session.Advanced.GetChangeVectorFor(user);
                    Assert.True(changeVector.Contains("A:1-"));
                    Assert.True(changeVector.Contains("B:2-"));
                    Assert.True(changeVector.Contains("C:1-"));
                }
            }
        }

        [NightlyBuildFact]
        public async Task ReplicateToWatcherWithAuth()
        {
            var serverCertPath = SetupServerAuthentication();
            var dbName = GetDatabaseName();
            var adminCert = AskServerForClientCertificate(serverCertPath, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);
            var userCert1 = AskServerForClientCertificate(serverCertPath, new Dictionary<string, DatabaseAccess>
            {
                [dbName] = DatabaseAccess.Admin
            });
            var userCert2 = AskServerForClientCertificate(serverCertPath, new Dictionary<string, DatabaseAccess>
            {
                [dbName] = DatabaseAccess.Admin
            });

            using (var store1 = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = userCert1,
                ModifyDatabaseName = s => dbName
            }))
            using (var store2 = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = userCert2,
                ModifyDatabaseName = s => dbName,
                CreateDatabase = false
            }))
            {
                var watcher2 = new ExternalReplication(store2.Urls)
                {
                    Database = store2.Database,
                };

                await AddWatcherToReplicationTopology(store1, watcher2);

                using (var session = store1.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Karmel" }, "users/1");
                    await session.SaveChangesAsync();
                }

                Assert.True(WaitForDocument<User>(store2, "users/1", (u) => u.Name == "Karmel"));
            }
        }

        [NightlyBuildFact]
        public async Task ReplicateToWatcherWithInvalidAuth()
        {
            var serverCertPath = SetupServerAuthentication();
            var dbName = GetDatabaseName();
            var adminCert = AskServerForClientCertificate(serverCertPath, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);
            var userCert1 = AskServerForClientCertificate(serverCertPath, new Dictionary<string, DatabaseAccess>
            {
                [dbName] = DatabaseAccess.Admin
            });
            var userCert2 = AskServerForClientCertificate(serverCertPath, new Dictionary<string, DatabaseAccess>
            {
                [dbName + "otherstuff"] = DatabaseAccess.Admin
            });

            using (var store1 = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = userCert1,
                ModifyDatabaseName = s => dbName
            }))
            using (var store2 = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = userCert2,
                ModifyDatabaseName = s => dbName,
                CreateDatabase = false
            }))
            {
                var watcher2 = new ExternalReplication(store2.Urls)
                {
                    Database = store2.Database,
                };

                await AddWatcherToReplicationTopology(store1, watcher2);

                using (var session = store1.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Karmel" }, "users/1");
                    await session.SaveChangesAsync();
                }

                Assert.Throws<AuthorizationException>(() => WaitForDocument<User>(store2, "users/1", (u) => u.Name == "Karmel"));
            }
        }

        [Fact]
        public async Task ExternalReplicationFailover()
        {
            var clusterSize = 3;
            var srcLeader = await CreateRaftClusterAndGetLeader(clusterSize);
            var dstLeader = await CreateRaftClusterAndGetLeader(clusterSize);

            var dstDB = GetDatabaseName();
            var srcDB = GetDatabaseName();

            var dstTopology = await CreateDatabaseInCluster(dstDB, clusterSize, dstLeader.WebUrl);
            var srcTopology = await CreateDatabaseInCluster(srcDB, clusterSize, srcLeader.WebUrl);

            var srcStore = new DocumentStore()
            {
                Urls = new[] {srcLeader.WebUrl},
                Database = srcDB,
            }.Initialize();

            using (var session = srcStore.OpenSession())
            {
                session.Store(new User { Name = "Karmel" }, "users/1");
                session.SaveChanges();
                Assert.True(await WaitForDocumentInClusterAsync<User>(
                    session as DocumentSession,
                    "users/1",
                    u => u.Name.Equals("Karmel"),
                    TimeSpan.FromSeconds(clusterSize + 5)));
            }

            // add watcher with invalid url to test the failover on database topology discovery
            var watcher = new ExternalReplication(new[] { "http://127.0.0.1:1234",dstLeader.WebUrl })
            {
                Database = dstDB
            };
            await AddWatcherToReplicationTopology((DocumentStore)srcStore, watcher);

            var dstStore = new DocumentStore
            {
                Urls = new[] { dstLeader.WebUrl},
                Database = watcher.Database,
            }.Initialize();

            var dstSession = dstStore.OpenSession();
            dstSession.Load<User>("Karmel");
            Assert.True(await WaitForDocumentInClusterAsync<User>(
                dstSession as DocumentSession,
                "users/1",
                u => u.Name.Equals("Karmel"),
                TimeSpan.FromSeconds(60)));


            Assert.True(WaitForValue(() => OngoingTasksHandler.GetOngoingTasksFor(srcDB, srcLeader.ServerStore).OngoingTasksList.Single(t => t is OngoingTaskReplication).As<OngoingTaskReplication>().DestinationUrl != null, true));

            var watcherTaskUrl = OngoingTasksHandler.GetOngoingTasksFor(srcDB, srcLeader.ServerStore).OngoingTasksList.Single(t => t is OngoingTaskReplication).As<OngoingTaskReplication>().DestinationUrl;

            // fail the node to to where the data is sent
            DisposeServerAndWaitForFinishOfDisposal(Servers.Single(s=>s.WebUrl == watcherTaskUrl));

            using (var session = srcStore.OpenSession())
            {
                session.Store(new User { Name = "Karmel2" }, "users/2");
                session.SaveChanges();
                Assert.True(await WaitForDocumentInClusterAsync<User>(
                    session as DocumentSession,
                    "users/2",
                    u => u.Name.Equals("Karmel2"),
                    TimeSpan.FromSeconds(clusterSize + 5)));
            }
            WaitForUserToContinueTheTest(srcStore as DocumentStore);

            Assert.True(WaitForDocument(dstStore, "users/2"));

            srcStore.Dispose();
            dstSession.Dispose();
            dstStore.Dispose();
        }
    }
}
