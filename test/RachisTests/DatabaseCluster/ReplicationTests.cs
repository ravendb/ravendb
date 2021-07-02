using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Security;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Commands;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Server;
using Raven.Server.Documents;
using Raven.Server.Documents.Replication;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace RachisTests.DatabaseCluster
{
    public class ReplicationTests : ReplicationTestBase
    {
        public ReplicationTests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task WaitForCommandToApply()
        {
            var clusterSize = 5;
            var databaseName = GetDatabaseName();
            var (_, leader) = await CreateRaftCluster(clusterSize, false, 0);
            using (var store = new DocumentStore
            {
                Urls = new[] { leader.WebUrl },
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
                var res = await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(doc));
                Assert.NotEqual(res.Topology.Members.First(), leader.ServerStore.NodeTag);

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User());
                    await session.SaveChangesAsync();
                }
            }
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task EnsureDocumentsReplication(bool useSsl)
        {
            var clusterSize = 5;
            var databaseName = GetDatabaseName();

            RavenServer leader;
            X509Certificate2 adminCertificate = null;
            X509Certificate2 clientCertificate = null;

            if (useSsl)
            {
                var result = await CreateRaftClusterWithSsl(clusterSize, false);
                leader = result.Leader;

                adminCertificate = RegisterClientCertificate(result.Certificates.ServerCertificate.Value, result.Certificates.ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin, server: leader);
                clientCertificate = RegisterClientCertificate(result.Certificates.ServerCertificate.Value, result.Certificates.ClientCertificate2.Value, new Dictionary<string, DatabaseAccess>
                {
                    [databaseName] = DatabaseAccess.Admin
                }, server: leader);
            }
            else
            {
                var result = await CreateRaftCluster(clusterSize, false);
                leader = result.Leader;
            }

            DatabasePutResult databaseResult;
            using (var store = new DocumentStore
            {
                Urls = new[] { leader.WebUrl },
                Database = databaseName,
                Certificate = adminCertificate,
                Conventions =
                {
                    DisableTopologyUpdates = true
                }
            }.Initialize())
            {
                var doc = new DatabaseRecord(databaseName);
                databaseResult = await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(doc, clusterSize));
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
                    TimeSpan.FromSeconds(60),
                    certificate: clientCertificate));
            }
        }

        
        [Fact]
        public async Task FailoverReplicationShouldFindEtagFromChangeVector()
        {
            var clusterSize = 3;
            var cluster = await CreateRaftCluster(clusterSize, watcherCluster: true);

            using (var source = GetDocumentStore(new Options {ReplicationFactor = clusterSize, Server = cluster.Leader}))
            using (var dest = GetDocumentStore(new Options {ReplicationFactor = 1, Server = cluster.Leader}))
            {
                await WaitAndAssertForValueAsync(() => GetMembersCount(source, source.Database), 3);

                var list = await SetupReplicationAsync(source, cluster.Nodes[0].ServerStore.NodeTag, dest);
                Assert.Equal(list.Count, 1);
                var exReplication = list[0];
                Assert.Equal(exReplication.ResponsibleNode, cluster.Nodes[0].ServerStore.NodeTag);

                using (var session = source.OpenAsyncSession())
                {
                    await session.StoreAsync(new User(), "foo/bar");
                    await session.SaveChangesAsync();
                }

                WaitForDocument(dest, "foo/bar");

                var node = dest.GetRequestExecutor().Topology.Nodes.Single();
                var server = Servers.Single(s => s.ServerStore.NodeTag == node.ClusterTag);
                var destDb = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(dest.Database);

                // wait for the change vector on the dest to be synced
                await WaitAndAssertForValueAsync(() => destDb.ReadLastEtagAndChangeVector().ChangeVector.ToChangeVectorList().Count, 3);

                var otherNode = cluster.Nodes.First(x => x.ServerStore.NodeTag != exReplication.ResponsibleNode).ServerStore;
                var sourceDb = await otherNode.DatabasesLandlord.TryGetOrCreateResourceStore(source.Database);

                var fetched = 0;
                sourceDb.ReplicationLoader.ForTestingPurposesOnly().OnOutgoingReplicationStart = (o) =>
                {
                    if (o.Destination.Database == dest.Database)
                    {
                        o.ForTestingPurposesOnly().OnDocumentSenderFetchNewItem = () =>
                        {
                            fetched++;
                        };
                    }
                };

                // change replication node
                var otherNodeTag = otherNode.NodeTag;
                var op = new UpdateExternalReplicationOperation(new ExternalReplication(dest.Database, $"ConnectionString-{dest.Identifier}")
                {
                    TaskId = exReplication.TaskId, MentorNode = otherNodeTag
                });

                var update = await source.Maintenance.SendAsync(op);
                await WaitForRaftIndexToBeAppliedInCluster(update.RaftCommandIndex);

                await WaitAndAssertForValueAsync(() =>
                {
                    return Servers.SelectMany(s =>
                    {
                        var db = s.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(source.Database).Result;
                        return db.ReplicationLoader.OutgoingHandlers.Where(h => h.Destination.Database == dest.Database);
                    }).Single()._parent._server.NodeTag;
                }, otherNodeTag);

                using (var session = source.OpenAsyncSession())
                {
                    await session.StoreAsync(new User(), "foo/bar/2");
                    await session.SaveChangesAsync();
                }

                WaitForDocument(dest, "foo/bar/2");
                Assert.Equal(1, fetched);
            }
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task EnsureReplicationToWatchers(bool useSsl)
        {
            var clusterSize = 3;
            var databaseName = GetDatabaseName();

            RavenServer leader;
            X509Certificate2 adminCertificate = null;

            if (useSsl)
            {
                var result = await CreateRaftClusterWithSsl(clusterSize);
                leader = result.Leader;

                adminCertificate = RegisterClientCertificate(result.Certificates, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin, server: leader);
            }
            else
            {
                var result = await CreateRaftCluster(clusterSize);
                leader = result.Leader;
            }

            var watchers = new List<ExternalReplication>();
            var watcherUrls = new Dictionary<string, string[]>();

            using (var store = new DocumentStore()
            {
                Urls = new[] { leader.WebUrl },
                Database = databaseName,
                Certificate = adminCertificate,
                Conventions =
                {
                    DisableTopologyUpdates = true
                }
            }.Initialize())
            {
                var doc = new DatabaseRecord(databaseName);
                var databaseResult = await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(doc, clusterSize));
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
                    TimeSpan.FromSeconds(60),
                    adminCertificate));
                for (var i = 0; i < 5; i++)
                {
                    var dbName = $"Watcher{i}";
                    doc = new DatabaseRecord(dbName);
                    var res = await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(doc));
                    watcherUrls.Add(dbName, res.NodesAddedTo.ToArray());
                    var server = Servers.Single(x => x.WebUrl == res.NodesAddedTo[0]);
                    await server.ServerStore.Cluster.WaitForIndexNotification(res.RaftCommandIndex);
                    await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(dbName);

                    var watcher = new ExternalReplication(dbName, $"{dbName}-Connection");
                    await AddWatcherToReplicationTopology((DocumentStore)store, watcher);
                    watchers.Add(watcher);
                }
            }

            foreach (var watcher in watchers)
            {
                using (var store = new DocumentStore
                {
                    Urls = watcherUrls[watcher.Database],
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

            var count = (await GetOngoingTasks(databaseName)).Count(t => t is OngoingTaskReplication);

            Assert.Equal(5, count);
        }

        private bool WaitForDocument(string[] urls, string database)
        {
            using (var store = new DocumentStore
            {
                Urls = urls,
                Database = database
            }.Initialize())
            {
                return WaitForDocument<User>(store, "users/1", u => u.Name == "Karmel", timeout: 15_000);
            }
        }

        private async Task<List<OngoingTask>> GetOngoingTasks(string name)
        {
            var tasks = new Dictionary<long, OngoingTask>();
            foreach (var server in Servers)
            {
                var handler = await InstantiateOutgoingTaskHandler(name, server);
                foreach (var task in handler.GetOngoingTasksInternal().OngoingTasksList)
                {
                    if (tasks.ContainsKey(task.TaskId) == false && task.TaskConnectionStatus != OngoingTaskConnectionStatus.NotOnThisNode)
                        tasks.Add(task.TaskId, task);
                }
            }
            return tasks.Values.ToList();
        }

        [Fact]
        public async Task SetMentorToExternalReplication()
        {
            var clusterSize = 5;
            var databaseName = GetDatabaseName();
            var (_, leader) = await CreateRaftCluster(clusterSize);
            var watchers = new List<ExternalReplication>();

            var watcherUrls = new Dictionary<string, string[]>();

            using (var store = new DocumentStore()
            {
                Urls = new[] { leader.WebUrl },
                Database = databaseName,
            }.Initialize())
            {
                var doc = new DatabaseRecord(databaseName);
                var databaseResult = await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(doc, clusterSize));
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
                    var dbName = $"Watcher{i}";
                    doc = new DatabaseRecord(dbName);
                    var res = await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(doc));
                    var server = Servers.Single(x => x.WebUrl == res.NodesAddedTo[0]);
                    await server.ServerStore.Cluster.WaitForIndexNotification(res.RaftCommandIndex);
                    await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(dbName);
                    watcherUrls.Add(dbName, res.NodesAddedTo.ToArray());
                    var watcher = new ExternalReplication(dbName, $"{dbName}-Connection")
                    {
                        MentorNode = "C"
                    };
                    watchers.Add(watcher);

                    var taskRes = await AddWatcherToReplicationTopology((DocumentStore)store, watcher);
                    var replicationResult = (OngoingTaskReplication)await GetTaskInfo((DocumentStore)store, taskRes.TaskId, OngoingTaskType.Replication);
                    Assert.Equal("C", replicationResult.ResponsibleNode.NodeTag);
                }
            }

            foreach (var watcher in watchers)
            {
                using (var store = new DocumentStore
                {
                    Urls = watcherUrls[watcher.Database],
                    Database = watcher.Database,
                }.Initialize())
                {
                    Assert.True(WaitForDocument<User>(store, "users/1", u => u.Name == "Karmel"));
                }
            }
        }

        [Fact]
        public async Task CanAddAndModifySingleWatcher()
        {
            var clusterSize = 3;
            var databaseName = GetDatabaseName();
            var external1 = GetDatabaseName();
            var external2 = GetDatabaseName();
            var (_, leader) = await CreateRaftCluster(clusterSize);
            ExternalReplication watcher;

            string[] watcherUrls;
            RavenServer watcherNode;
            using (var store = new DocumentStore()
            {
                Urls = new[] { leader.WebUrl },
                Database = databaseName,
                Conventions =
                {
                    DisableTopologyUpdates = true
                }
            }.Initialize())
            {
                var doc = new DatabaseRecord(databaseName);
                var databaseResult = await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(doc, clusterSize));
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

                doc = new DatabaseRecord(external1);
                var res = await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(doc));
                watcherUrls = res.NodesAddedTo.ToArray();
                watcherNode = Servers.Single(x => x.WebUrl == res.NodesAddedTo[0]);
                await watcherNode.ServerStore.Cluster.WaitForIndexNotification(res.RaftCommandIndex);
                await watcherNode.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(external1);

                watcher = new ExternalReplication(external1, "Watcher-Connection")
                {
                    Name = "MyExternalReplication1",
                    MentorNode = leader.ServerStore.NodeTag
                };
                var updateRes = await AddWatcherToReplicationTopology((DocumentStore)store, watcher);
                await WaitForRaftIndexToBeAppliedInCluster(updateRes.RaftCommandIndex, TimeSpan.FromSeconds(10));
                Assert.True(WaitForDocument(new[] { leader.WebUrl }, watcher.Database));
            }

            var handler = await InstantiateOutgoingTaskHandler(databaseName, leader);
            var tasks = handler.GetOngoingTasksInternal().OngoingTasksList;
            Assert.Equal(1, tasks.Count);
            var repTask = tasks[0] as OngoingTaskReplication;
            Assert.Equal(repTask?.DestinationDatabase, watcher.Database);
            Assert.Equal(watcherNode.ServerStore.GetNodeHttpServerUrl(), repTask?.DestinationUrl);
            Assert.Equal(repTask?.TaskName, watcher.Name);

            watcher.TaskId = Convert.ToInt64(repTask?.TaskId);

            using (var store = new DocumentStore
            {
                Urls = watcherUrls,
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
                Urls = new[] { leader.WebUrl },
                Database = databaseName,
                Conventions =
                {
                    DisableTopologyUpdates = true
                }
            }.Initialize())
            {
                var doc = new DatabaseRecord(external2);
                var res = await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(doc));
                watcherNode = Servers.Single(x => x.WebUrl == res.NodesAddedTo[0]);
                await watcherNode.ServerStore.Cluster.WaitForIndexNotification(res.RaftCommandIndex);
                await watcherNode.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(external2);
                watcherUrls = res.NodesAddedTo.ToArray();

                //modify watcher
                watcher.Name = "MyExternalReplication2";
                watcher.Database = external2;
                var updateRes = await AddWatcherToReplicationTopology((DocumentStore)store, watcher);
                await WaitForRaftIndexToBeAppliedInCluster(updateRes.RaftCommandIndex, TimeSpan.FromSeconds(10));
                Assert.True(WaitForDocument(new[] { leader.WebUrl }, watcher.Database));
            }

            tasks = handler.GetOngoingTasksInternal().OngoingTasksList;
            Assert.Equal(1, tasks.Count);
            repTask = tasks[0] as OngoingTaskReplication;
            Assert.Equal(repTask?.DestinationDatabase, external2);
            Assert.Equal(repTask?.DestinationUrl, watcherNode.ServerStore.GetNodeHttpServerUrl());
            Assert.Equal(repTask?.TaskName, "MyExternalReplication2");

            using (var store = new DocumentStore
            {
                Urls = watcherUrls,
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
                Urls = new[] { leader.WebUrl },
                Database = databaseName,
                Conventions =
                {
                    DisableTopologyUpdates = true
                }
            }.Initialize())
            {
                await DeleteOngoingTask((DocumentStore)store, watcher.TaskId, OngoingTaskType.Replication);
                tasks = await GetOngoingTasks(databaseName);
                Assert.Equal(0, tasks.Count);
            }
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task DoNotReplicateBack(bool useSsl)
        {
            var clusterSize = 5;
            var databaseName = GetDatabaseName();

            RavenServer leader;
            X509Certificate2 adminCertificate = null;

            if (useSsl)
            {
                var result = await CreateRaftClusterWithSsl(clusterSize);
                leader = result.Leader;

                adminCertificate = RegisterClientCertificate(result.Certificates, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin, server: leader);
            }
            else
            {
                var result = await CreateRaftCluster(clusterSize);
                leader = result.Leader;
            }

            using (var store = new DocumentStore
            {
                Urls = new[] { leader.WebUrl },
                Database = databaseName,
                Certificate = adminCertificate,
                Conventions =
                {
                    DisableTopologyUpdates = true
                }
            }.Initialize())
            {
                var doc = new DatabaseRecord(databaseName);
                var databaseResult = await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(doc, clusterSize));
                var topology = databaseResult.Topology;
                Assert.Equal(clusterSize, topology.AllNodes.Count());

                await WaitForValueOnGroupAsync(topology, s =>
               {
                   var db = s.DatabasesLandlord.TryGetOrCreateResourceStore(databaseName).Result;
                   return db.ReplicationLoader?.OutgoingConnections.Count();
               }, clusterSize - 1, 60000);

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
                    TimeSpan.FromSeconds(60),
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

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task AddGlobalChangeVectorToNewDocument(bool useSsl)
        {
            var clusterSize = 3;
            var databaseName = GetDatabaseName();

            RavenServer leader;
            X509Certificate2 adminCertificate = null;
            X509Certificate2 clientCertificate = null;

            if (useSsl)
            {
                var result = await CreateRaftClusterWithSsl(clusterSize, true, 0);
                leader = result.Leader;

                adminCertificate = RegisterClientCertificate(result.Certificates.ServerCertificate.Value, result.Certificates.ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin, server: leader);
                clientCertificate = RegisterClientCertificate(result.Certificates.ServerCertificate.Value, result.Certificates.ClientCertificate2.Value, new Dictionary<string, DatabaseAccess>
                {
                    [databaseName] = DatabaseAccess.Admin
                }, server: leader);
            }
            else
            {
                var result = await CreateRaftCluster(clusterSize, true, 0);
                leader = result.Leader;
            }

            var doc = new DatabaseRecord(databaseName);
            using (var store = new DocumentStore
            {
                Urls = new[] { leader.WebUrl },
                Database = databaseName,
                Certificate = adminCertificate,
                Conventions =
                {
                    DisableTopologyUpdates = true
                }
            }.Initialize())
            {
                var databaseResult = await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(doc, clusterSize));
                var topology = databaseResult.Topology;
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
                    databaseResult.Topology,
                    databaseName,
                    "users/1",
                    u => u.Name.Equals("Karmel"),
                    TimeSpan.FromSeconds(60),
                    certificate: clientCertificate));

                // we need to wait for database change vector to be updated
                // which means that we need to wait for replication to do a full mesh propagation
                try
                {
                    await WaitForValueOnGroupAsync(topology, serverStore =>
                    {
                        var database = serverStore.DatabasesLandlord.TryGetOrCreateResourceStore(databaseName).Result;

                        using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                        using (context.OpenReadTransaction())
                        {
                            var cv = DocumentsStorage.GetDatabaseChangeVector(context);

                            return cv != null && cv.Contains("A:1-") && cv.Contains("B:1-") && cv.Contains("C:1-");
                        }
                    }, expected: true, timeout: 60000);
                }
                catch (Exception e)
                {
                    var error = e.Message;
                    foreach (var node in topology.AllNodes)
                    {
                        var serverStore = Servers.Single(s => s.ServerStore.NodeTag == node).ServerStore;
                        var database = serverStore.DatabasesLandlord.TryGetOrCreateResourceStore(databaseName).Result;
                        using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                        using (context.OpenReadTransaction())
                        {
                            var cv = DocumentsStorage.GetDatabaseChangeVector(context);

                            error += $" {node}: {cv}.";
                        }

                        foreach (var item in database.ReplicationLoader.OutgoingFailureInfo)
                        {
                            error += $"ErrorsCount: {item.Value.Errors.Count}. Exception: ";
                            foreach (var err in item.Value.Errors)
                            {
                                error += $"{err.Message} , ";
                            }
                            error += $"NextTimeout: {item.Value.NextTimeout}. " +
                                     $"RetryOn: {item.Value.RetryOn}. " +
                                     $"External: {item.Value.RetryOn}." +
                                     $"DestinationDbId: {item.Value.DestinationDbId}." +
                                     $"LastHeartbeatTicks: {item.Value.LastHeartbeatTicks}. ";

                        };
                    }

                    throw new Exception(error);
                }
            }

            using (var store = new DocumentStore()
            {
                Urls = new[] { Servers[1].WebUrl },
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
                    Assert.True(changeVector.Contains("A:1-"), $"No A:1- in {changeVector}");
                    Assert.True(changeVector.Contains("B:2-"), $"No B:1- in {changeVector}");
                    Assert.True(changeVector.Contains("C:1-"), $"No C:1- in {changeVector}");
                }
            }
        }

        [Fact]
        public async Task ReplicateToWatcherWithAuth()
        {
            var certificates = SetupServerAuthentication();
            var dbName = GetDatabaseName();
            var adminCert = RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);
            var opCert = RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate2.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.Operator);

            using (var store1 = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = opCert,
                ModifyDatabaseName = s => dbName
            }))
            using (var store2 = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = opCert,
                ModifyDatabaseName = s => dbName,
                CreateDatabase = false
            }))
            {
                var watcher2 = new ExternalReplication(store2.Database, "ConnectionString");

                await AddWatcherToReplicationTopology(store1, watcher2);

                using (var session = store1.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Karmel" }, "users/1");
                    await session.SaveChangesAsync();
                }

                Assert.True(WaitForDocument<User>(store2, "users/1", (u) => u.Name == "Karmel"));
            }
        }

        [Fact]
        public async Task ReplicateToWatcherWithInvalidAuth()
        {
            var certificates = SetupServerAuthentication();
            var dbName = GetDatabaseName();
            var adminCert = RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);
            var userCert1 = RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate2.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.Operator);
            var userCert2 = RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate3.Value, new Dictionary<string, DatabaseAccess>
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
                var watcher2 = new ExternalReplication(store2.Database, "ConnectionString");

                await AddWatcherToReplicationTopology(store1, watcher2);

                using (var session = store1.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Karmel" }, "users/1");
                    await session.SaveChangesAsync();
                }

                var value = WaitForValue(() =>
                {
                    try
                    {
                        using (var session = store2.OpenSession())
                        {
                            var user = session.Load<User>("users/1");
                        }

                        return false;
                    }
                    catch (AuthorizationException)
                    {
                        return true;
                    }
                }, true);

                Assert.True(value);
            }
        }

        [Fact]
        public async Task ExternalReplicationFailover()
        {
            var clusterSize = 3;
            var (_, srcLeader) = await CreateRaftCluster(clusterSize);
            var (_, dstLeader) = await CreateRaftCluster(clusterSize);

            var dstDB = GetDatabaseName();
            var srcDB = GetDatabaseName();

            var dstTopology = await CreateDatabaseInCluster(dstDB, clusterSize, dstLeader.WebUrl);
            var srcTopology = await CreateDatabaseInCluster(srcDB, clusterSize, srcLeader.WebUrl);

            using (var srcStore = new DocumentStore()
            {
                Urls = new[] { srcLeader.WebUrl },
                Database = srcDB,
            }.Initialize())
            {
                using (var session = srcStore.OpenSession())
                {
                    session.Advanced.WaitForReplicationAfterSaveChanges(timeout: TimeSpan.FromSeconds(30), replicas: clusterSize - 1);
                    session.Store(new User
                    {
                        Name = "Karmel"
                    }, "users/1");
                    session.SaveChanges();
                }

                // add watcher with invalid url to test the failover on database topology discovery
                var watcher = new ExternalReplication(dstDB, "connection")
                {
                    MentorNode = "B"
                };
                await AddWatcherToReplicationTopology((DocumentStore)srcStore, watcher, new[] { "http://127.0.0.1:1234", dstLeader.WebUrl });

                using (var dstStore = new DocumentStore
                {
                    Urls = new[] { dstLeader.WebUrl },
                    Database = watcher.Database,
                }.Initialize())
                {
                    using (var dstSession = dstStore.OpenSession())
                    {
                        dstSession.Load<User>("Karmel");
                        Assert.True(await WaitForDocumentInClusterAsync<User>(
                            dstSession as DocumentSession,
                            "users/1",
                            u => u.Name.Equals("Karmel"),
                            TimeSpan.FromSeconds(60)));
                    }

                    var responsibale = srcLeader.ServerStore.GetClusterTopology().GetUrlFromTag("B");
                    var server = Servers.Single(s => s.WebUrl == responsibale);
                    var handler = await InstantiateOutgoingTaskHandler(srcDB, server);
                    Assert.True(WaitForValue(
                        () => ((OngoingTaskReplication)handler.GetOngoingTasksInternal().OngoingTasksList.Single(t => t is OngoingTaskReplication)).DestinationUrl !=
                              null,
                        true));

                    var watcherTaskUrl = ((OngoingTaskReplication)handler.GetOngoingTasksInternal().OngoingTasksList.Single(t => t is OngoingTaskReplication))
                        .DestinationUrl;

                    // fail the node to to where the data is sent
                    DisposeServerAndWaitForFinishOfDisposal(Servers.Single(s => s.WebUrl == watcherTaskUrl));

                    using (var session = srcStore.OpenSession())
                    {
                        session.Advanced.WaitForReplicationAfterSaveChanges(timeout: TimeSpan.FromSeconds(30), replicas: clusterSize - 1);
                        session.Store(new User
                        {
                            Name = "Karmel2"
                        }, "users/2");
                        session.SaveChanges();
                    }

                    Assert.True(WaitForDocument(dstStore, "users/2", 30_000));
                }
            }
        }

        [Fact]
        public async Task GetFirstTopologyShouldTimeout()
        {
            var clusterSize = 1;
            var (_, srcLeader) = await CreateRaftCluster(clusterSize);
            var (_, dstLeader) = await CreateRaftCluster(clusterSize);

            var dstDB = GetDatabaseName();
            var srcDB = GetDatabaseName();

            var dstTopology = await CreateDatabaseInCluster(dstDB, clusterSize, dstLeader.WebUrl);
            var srcTopology = await CreateDatabaseInCluster(srcDB, clusterSize, srcLeader.WebUrl);

            using (var srcStore = new DocumentStore()
            {
                Urls = new[] { srcLeader.WebUrl },
                Database = srcDB,
            }.Initialize())
            using (var dstStore = new DocumentStore
            {
                Urls = new[] { dstLeader.WebUrl },
                Database = dstDB,
            }.Initialize())
            {
                dstLeader.ServerStore.InitializationCompleted.Reset(true);
                dstLeader.ServerStore.Initialized = false;

                try
                {
                    var db = await srcLeader.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(srcDB);
                    var ex = await Assert.ThrowsAsync<AggregateException>(async () =>
                    {
                        var wait = Task.Delay(TimeSpan.FromSeconds(30));
                        var exec = Task.Run(() =>
                        {
                            using (var requestExecutor = RequestExecutor.Create(new[] { dstLeader.WebUrl }, dstDB, null, DocumentConventions.DefaultForServer))
                            using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out JsonOperationContext ctx))
                            {
                                var cmd = new GetTcpInfoCommand("external-replication", db.Name, db.DbId.ToString(), db.ReadLastEtag());
                                requestExecutor.Execute(cmd, ctx);
                            }
                        });

                        var t = await Task.WhenAny(exec, wait);
                        await t;
                    });
                    Assert.Contains("failed with timeout after 00:00:15", ex.Message);
                }
                finally
                {
                    dstLeader.ServerStore.Initialized = true;
                    dstLeader.ServerStore.InitializationCompleted.Set();
                }
            }
        }

        [Fact]
        public async Task GetTcpInfoShouldTimeout()
        {
            var clusterSize = 1;
            var (_, srcLeader) = await CreateRaftCluster(clusterSize);
            var (_, dstLeader) = await CreateRaftCluster(clusterSize);

            var dstDB = GetDatabaseName();
            var srcDB = GetDatabaseName();

            await CreateDatabaseInCluster(dstDB, clusterSize, dstLeader.WebUrl);
            await CreateDatabaseInCluster(srcDB, clusterSize, srcLeader.WebUrl);

            using (var srcStore = new DocumentStore()
            {
                Urls = new[] { srcLeader.WebUrl },
                Database = srcDB,
            }.Initialize())
            using (var dstStore = new DocumentStore
            {
                Urls = new[] { dstLeader.WebUrl },
                Database = dstDB,
            }.Initialize())
            {
                try
                {
                    var db = await srcLeader.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(srcDB);
                    var ex = await Assert.ThrowsAsync<RavenException>(async () =>
                    {
                        var wait = Task.Delay(TimeSpan.FromSeconds(30));
                        var exec = Task.Run(() =>
                        {
                            using (var requestExecutor = RequestExecutor.Create(new[] { dstLeader.WebUrl }, dstDB, null,
                                DocumentConventions.DefaultForServer))
                            using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out JsonOperationContext ctx))
                            {
                                var cmd = new GetTcpInfoCommand("external-replication", dstDB, db.DbId.ToString(), db.ReadLastEtag());
                                requestExecutor.Execute(cmd, ctx);

                                dstLeader.ServerStore.InitializationCompleted.Reset(true);
                                dstLeader.ServerStore.Initialized = false;

                                cmd = new GetTcpInfoCommand("external-replication", dstDB, db.DbId.ToString(), db.ReadLastEtag());
                                requestExecutor.Execute(cmd, ctx);
                            }
                        });

                        var t = await Task.WhenAny(exec, wait);
                        await t;
                    });

                    Assert.Contains("failed with timeout after 00:00:15", ex.Message);
                }
                finally
                {
                    dstLeader.ServerStore.Initialized = true;
                    dstLeader.ServerStore.InitializationCompleted.Set();
                }
            }
        }

        [Fact]
        public async Task RavenDB_14284()
        {
            using (var store1 = GetDocumentStore())
            using (var store2 = GetDocumentStore())
            {
                var database = await GetDocumentDatabaseInstanceFor(store1);
                var handlers = new HashSet<OutgoingReplicationHandler>();

                database.ReplicationLoader.OutgoingReplicationAdded += handler =>
                {
                    handlers.Add(handler);
                };

                var databaseWatcher1 = new ExternalReplication(store2.Database, $"ConnectionString-{store1.Identifier}_1");
                await AddWatcherToReplicationTopology(store1, databaseWatcher1, store1.Urls);

                var databaseWatcher2 = new ExternalReplication(store2.Database, $"ConnectionString-{store1.Identifier}_2");
                await AddWatcherToReplicationTopology(store1, databaseWatcher2, store1.Urls);

                await WaitForValueAsync(() =>
                {
                    foreach (var handler in database.ReplicationLoader.OutgoingHandlers)
                    {
                        handlers.Add(handler);
                    }

                    return handlers.Count;
                }, 2);

                Assert.Equal(2, handlers.Count);

                EnsureReplicating(store1, store2);

                var list = handlers.ToList();
                var connection1Task = WaitForValueAsync(() => list[0].IsConnectionDisposed, true, timeout: 5_000);
                var connection2Task = WaitForValueAsync(() => list[1].IsConnectionDisposed, true, timeout: 5_000);

                var connection2 = await connection2Task;
                var connection1 = await connection1Task;

                Assert.True(connection1 ^ connection2, $"connection 1 disposed={connection1}, connection 2 disposed={connection2}");
            }
        }

        [NightlyBuildFact]
        public async Task RavenDB_14435()
        {
            using (var src = GetDocumentStore())
            using (var dst = GetDocumentStore())
            {
                var database = await GetDocumentDatabaseInstanceFor(src);

                using (var session = src.OpenSession())
                {
                    session.Store(new User(), "foo/bar");
                    session.SaveChanges();
                }

                using (var controller = new ReplicationController(database))
                {
                    var databaseWatcher1 = new ExternalReplication(dst.Database, $"ConnectionString-{src.Identifier}_1");
                    await AddWatcherToReplicationTopology(src, databaseWatcher1, src.Urls);
                    controller.ReplicateOnce();

                    Assert.NotNull(WaitForDocumentToReplicate<User>(dst, "foo/bar", 10_000));
                    await Task.Delay(ReplicationLoader.MaxInactiveTime.Add(TimeSpan.FromSeconds(10)));

                    var databaseWatcher2 = new ExternalReplication(dst.Database, $"ConnectionString-{src.Identifier}_2");
                    await AddWatcherToReplicationTopology(src, databaseWatcher2, src.Urls);

                    await Task.Delay(TimeSpan.FromSeconds(5));
                }
                EnsureReplicating(src, dst);
            }
        }

        [Fact]
        public async Task ReplicateRaftDocuments()
        {
            var cluster = await CreateRaftCluster(2, watcherCluster: true);
            using (var store = GetDocumentStore(new Options
            {
                Server = cluster.Leader,
                ReplicationFactor = 1,
                ModifyDatabaseRecord = r => r.Topology = new DatabaseTopology { Members = new List<string> { cluster.Leader.ServerStore.NodeTag } }
            }))
            {
                var database = await cluster.Leader.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
                using (var controller = new ReplicationController(database))
                {
                    using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                    {
                        session.Store(new User(), "users/1");
                        session.Store(new User(), "users/2");
                        session.SaveChanges();
                    }

                    await Task.Delay(3000); // wait for cleanup
                    cluster.Leader.ServerStore.Observer.Suspended = true;
                    await store.Maintenance.Server.SendAsync(new AddDatabaseNodeOperation(store.Database));

                    using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                    {
                        session.Store(new User(), "users/3");
                        session.SaveChanges();
                    }
                    controller.ReplicateOnce();
                    Assert.False(await WaitForDocumentInClusterAsync<User>(new DatabaseTopology { Members = new List<string> { "A", "B" } }, store.Database, "users/3", null,
                        TimeSpan.FromSeconds(10)));
                }

                Assert.True(await WaitForDocumentInClusterAsync<User>(new DatabaseTopology { Members = new List<string> { "A", "B" } }, store.Database, "users/3", null,
                    TimeSpan.FromSeconds(10)));
                Assert.True(await WaitForDocumentInClusterAsync<User>(new DatabaseTopology { Members = new List<string> { "A", "B" } }, store.Database, "users/2", null,
                    TimeSpan.FromSeconds(10)));
                Assert.True(await WaitForDocumentInClusterAsync<User>(new DatabaseTopology { Members = new List<string> { "A", "B" } }, store.Database, "users/1", null,
                    TimeSpan.FromSeconds(10)));
            }
        }
    }
}
