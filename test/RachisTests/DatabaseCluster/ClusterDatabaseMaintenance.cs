using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using FastTests.Server.Replication;
using FastTests.Utils;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Documents.Session;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Commands;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Client.Util;
using Raven.Server.Config;
using Raven.Server.Config.Categories;
using Raven.Server.Documents;
using Raven.Server.Documents.Replication;
using Raven.Server.Rachis;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace RachisTests.DatabaseCluster
{
    public class ClusterDatabaseMaintenance : ReplicationTestBase
    {
        public ClusterDatabaseMaintenance(ITestOutputHelper output) : base(output)
        {
        }

        private class User
        {
            public string Name { get; set; }
            public string Email { get; set; }
            public int Age { get; set; }
        }

        private class UsersByName : AbstractIndexCreationTask<User>
        {
            public UsersByName()
            {
                Map = usersCollection => from user in usersCollection
                                         select new { user.Name };
                Index(x => x.Name, FieldIndexing.Search);
            }
        }

        [Fact]
        public void CreateDatabaseOn00000Node()
        {
            using (var server = GetNewServer(new ServerCreationOptions
            {
                CustomSettings = new Dictionary<string, string>
                {
                    [RavenConfiguration.GetKey(x => x.Core.ServerUrls)] = "http://0.0.0.0:0",
                    [RavenConfiguration.GetKey(x => x.Security.UnsecuredAccessAllowed)] = UnsecuredAccessAddressRange.PublicNetwork.ToString()
                },
                RegisterForDisposal = false
            }))
            using (var store = GetDocumentStore(new Options
            {
                Server = server,
                ModifyDocumentStore = documentStore => documentStore.Urls = new[] { server.ServerStore.GetNodeHttpServerUrl() },
                CreateDatabase = true,
                DeleteDatabaseOnDispose = true
            }))
            {
            }
        }

        [Fact]
        public async Task DontPurgeTombstonesWhenNodeIsDown()
        {
            var clusterSize = 3;
            var leader = await CreateRaftClusterAndGetLeader(clusterSize, leaderIndex: 0);
            using (var store = GetDocumentStore(new Options
            {
                CreateDatabase = true,
                ReplicationFactor = clusterSize,
                Server = leader,
                DeleteDatabaseOnDispose = false // we bring one node down, so we can't delete the entire database, but we run in mem, so we don't care
            }))
            {
                var index = new UsersByName();
                await index.ExecuteAsync(store);
                using (var session = store.OpenAsyncSession())
                {
                    session.Advanced.WaitForReplicationAfterSaveChanges(TimeSpan.FromSeconds(30), replicas: 2);
                    await session.StoreAsync(new User
                    {
                        Name = "Karmel"
                    }, "users/1");
                    await session.SaveChangesAsync();
                }
                await DisposeServerAndWaitForFinishOfDisposalAsync(Servers[1]);
                using (var session = store.OpenAsyncSession())
                {
                    session.Advanced.WaitForReplicationAfterSaveChanges(TimeSpan.FromSeconds(30), replicas: 1);
                    session.Delete("users/1");
                    await session.SaveChangesAsync();
                }

                WaitForIndexing(store);

                var database = await leader.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
                await database.TombstoneCleaner.ExecuteCleanup();
                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    Assert.Equal(2, database.DocumentsStorage.GetLastTombstoneEtag(ctx.Transaction.InnerTransaction, "Users"));
                }
            }
        }

        [Fact]
        public async Task KeepReplicationFactorOnRecordUpdate()
        {
            var clusterSize = 3;
            var cluster = await CreateRaftCluster(clusterSize, watcherCluster: true);
            using (var store = GetDocumentStore(new Options
            {
                Server = cluster.Leader,
                ReplicationFactor = clusterSize
            }))
            {
                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                record.Topology.Members.Remove("A");
                record.Topology.Rehabs.Add("A");

                await store.Maintenance.Server.SendAsync(new UpdateDatabaseOperation(record, record.Etag));
                var val = await WaitForValueAsync(async () => await GetMembersCount(store, store.Database), clusterSize);
                Assert.Equal(3, val);
            }
        }

        [Fact]
        public async Task MoveToRehabOnServerDown()
        {
            var clusterSize = 3;
            var databaseName = GetDatabaseName();
            var cluster = await CreateRaftCluster(clusterSize, true, 0, customSettings: new Dictionary<string, string>
            {
                [RavenConfiguration.GetKey(x => x.Cluster.MoveToRehabGraceTime)] = "4"
            });
            using (var store = new DocumentStore
            {
                Urls = new[] { cluster.Leader.WebUrl },
                Database = databaseName
            }.Initialize())
            {
                var doc = new DatabaseRecord(databaseName);
                var databaseResult = await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(doc, clusterSize));
                Assert.Equal(clusterSize, databaseResult.Topology.Members.Count);
                cluster.Nodes[1].Dispose();

                var val = await WaitForValueAsync(async () => await GetMembersCount(store, databaseName), clusterSize - 1);
                Assert.Equal(clusterSize - 1, val);
                val = await WaitForValueAsync(async () => await GetRehabCount(store, databaseName), 1);
                Assert.Equal(1, val);
            }
        }

        [Fact]
        public async Task MoveToRehabOnLargeGap()
        {
            var clusterSize = 3;
            DefaultClusterSettings[RavenConfiguration.GetKey(x => x.Cluster.MaxChangeVectorDistance)] = "1";
            var cluster = await CreateRaftCluster(clusterSize,watcherCluster: true);
            using (var store = GetDocumentStore(new Options
            {
                ReplicationFactor = 3,
                Server = cluster.Leader,
                ModifyDocumentStore = s => s.Conventions = new DocumentConventions
                {
                    DisableTopologyUpdates = true
                }
            }))
            {
                var broken = await BreakReplication(cluster.Leader.ServerStore, store.Database);
                var val = await WaitForValueAsync(async () => await GetMembersCount(store), 3);
                Assert.Equal(3, val);

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User(), "users/1");
                    await session.StoreAsync(new User(), "users/2");
                    await session.SaveChangesAsync();
                }

                WaitForUserToContinueTheTest(store);

                val = await WaitForValueAsync(async () => await GetMembersCount(store), 1);
                Assert.Equal(1, val);

                val = await WaitForValueAsync(async () => await GetRehabCount(store), 2);
                Assert.Equal(2, val);

                broken.Mend();

                val = await WaitForValueAsync(async () => await GetMembersCount(store), 3);
                Assert.Equal(3, val);

            }
        }

        [Fact]
        public async Task CanFixTopology()
        {
            var clusterSize = 3;
            var databaseName = GetDatabaseName();
            var settings = new Dictionary<string, string>
            {
                [RavenConfiguration.GetKey(x => x.Cluster.MoveToRehabGraceTime)] = "1"
            };
            var cluster = await CreateRaftCluster(clusterSize, false, 0, customSettings: settings);
            using (var store = new DocumentStore
            {
                Urls = new[] { cluster.Leader.WebUrl },
                Database = databaseName
            }.Initialize())
            {
                var order = new List<string> { "A", "B", "C" };
                var doc = new DatabaseRecord(databaseName)
                {
                    Topology = new DatabaseTopology
                    {
                        Members = new List<string> { "A", "B", "C" },
                        ReplicationFactor = 3,
                        PriorityOrder = order
                    }
                };

                var databaseResult = await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(doc, clusterSize));
                Assert.Equal(clusterSize, databaseResult.Topology.Members.Count);

                var node = cluster.Nodes.Single(n => n.ServerStore.NodeTag == "A");
                var result = await DisposeServerAndWaitForFinishOfDisposalAsync(node);

                var val = await WaitForValueAsync(async () => await GetRehabCount(store, databaseName), 1);
                Assert.Equal(1, val);

                settings[RavenConfiguration.GetKey(x => x.Core.ServerUrls)] = result.Url;

                cluster.Nodes[0] = GetNewServer(new ServerCreationOptions
                {
                    CustomSettings = settings,
                    DataDirectory = result.DataDirectory
                });

                val = await WaitForValueAsync(async () => await GetMembersCount(store, databaseName), 3);
                Assert.Equal(3, val);
                val = await WaitForValueAsync(async () => await GetRehabCount(store, databaseName), 0);
                Assert.Equal(0, val);

                var res = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(databaseName));
                Assert.Equal(order, res.Topology.Members);
            }
        }

        [Fact]
        public async Task ReshuffleAfterPromotion()
        {
            var numberOfDatabases = 25;
            var clusterSize = 3;
            var settings = new Dictionary<string,string>()
            {
                [RavenConfiguration.GetKey(x => x.Cluster.MoveToRehabGraceTime)] = "5",
            };
            var cluster = await CreateRaftCluster(clusterSize, false, 0, watcherCluster: true, customSettings: settings);
            using (var store = new DocumentStore { Urls = new[] { cluster.Leader.WebUrl } }.Initialize())
            {
                var names = new List<string>();
                for (int i = 0; i < numberOfDatabases; i++)
                {
                    var name = GetDatabaseName();
                    names.Add(name);
                    var doc = new DatabaseRecord(name);
                    var databaseResult = await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(doc, clusterSize));
                    Assert.Equal(clusterSize, databaseResult.Topology.Count);
                    await WaitForRaftIndexToBeAppliedInCluster(databaseResult.RaftCommandIndex, TimeSpan.FromSeconds(10));
                }

                var result = await DisposeServerAndWaitForFinishOfDisposalAsync(cluster.Nodes[2]);

                // wait for moving all of the nodes to rehab state
                foreach (string name in names)
                {
                    var val = await WaitForValueAsync(async () => await GetMembersCount(store, name), clusterSize - 1);
                    Assert.Equal(clusterSize - 1, val);
                    val = await WaitForValueAsync(async () => await GetRehabCount(store, name), 1);
                    Assert.Equal(1, val);
                }

                settings[RavenConfiguration.GetKey(x => x.Core.ServerUrls)] = result.Url;
                cluster.Nodes[2] = GetNewServer(new ServerCreationOptions
                {
                    DeletePrevious = true,
                    RunInMemory = false,
                    DataDirectory = result.DataDirectory,
                    CustomSettings = settings
                });

                var preferredCount = new Dictionary<string, int> { ["A"] = 0, ["B"] = 0, ["C"] = 0 };

                using (var requestExecutor = ClusterRequestExecutor.CreateForSingleNode(Servers[0].WebUrl, null))
                using (requestExecutor.ContextPool.AllocateOperationContext(out JsonOperationContext context))
                {
                    await WaitForValueAsync(async () =>
                    {
                        var clusterTopology = new GetClusterTopologyCommand();
                        await requestExecutor.ExecuteAsync(clusterTopology, context);
                        return clusterTopology.Result.Topology.Members.Count;
                    }, 3);
                }

                // wait for recovery of all of the nodes back to member
                var timeout = cluster.Leader.Configuration.Cluster.SupervisorSamplePeriod.AsTimeSpan * numberOfDatabases * 5;
                foreach (string name in names)
                {
                    var val = await WaitForValueAsync(async () => await GetMembersCount(store, name), clusterSize, (int)timeout.TotalMilliseconds);
                    Assert.Equal(clusterSize, val);

                    var res = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(name));
                    Assert.Equal(clusterSize, res.Topology.Members.Count);

                    var preferred = res.Topology.Members[0];
                    preferredCount[preferred]++;
                }

                Assert.True(preferredCount["A"] > 1);
                Assert.True(preferredCount["B"] > 1);
                Assert.True(preferredCount["C"] > 1);
            }
        }

        [Fact]
        public async Task MoveLoadingNodeToLast()
        {
            var clusterSize = 3;
            var settings = new Dictionary<string, string>()
            {
                [RavenConfiguration.GetKey(x => x.Cluster.ElectionTimeout)] = 300.ToString(),
                [RavenConfiguration.GetKey(x => x.Cluster.StabilizationTime)] = "1",
                [RavenConfiguration.GetKey(x => x.Cluster.MoveToRehabGraceTime)] = "10",
                [RavenConfiguration.GetKey(x => x.Cluster.RotatePreferredNodeGraceTime)] = "1",
                [RavenConfiguration.GetKey(x => x.Replication.ReplicationMinimalHeartbeat)] = "15",
            };

            var cluster = await CreateRaftCluster(clusterSize, false, 0, watcherCluster: true, customSettings: settings);
            using (var store = GetDocumentStore(new Options
            {
                Server = cluster.Leader,
                ReplicationFactor = clusterSize
            }))
            {
                var tcs = new TaskCompletionSource<DocumentDatabase>();

                var databaseName = store.Database;
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Karmel" }, "users/1");
                    session.SaveChanges();

                    Assert.True(await WaitForDocumentInClusterAsync<User>((DocumentSession)session, "users/1", _ => true, TimeSpan.FromSeconds(5)));
                }

                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                var preferred = Servers.Single(s => s.ServerStore.NodeTag == record.Topology.Members[0]);

                int val;
                using (new DisposableAction(() =>
                {
                    preferred.ServerStore.DatabasesLandlord.DatabasesCache.TryRemove(databaseName, out var t);
                    if (t == tcs.Task)
                        tcs.SetCanceled();
                }))
                {
                    var t = preferred.ServerStore.DatabasesLandlord.DatabasesCache.Replace(databaseName, tcs.Task);
                    t.Result.Dispose();

                    Assert.True(await WaitForValueAsync(async () =>
                    {
                        record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                        return record.Topology.Members[0] != preferred.ServerStore.NodeTag;
                    }, true));

                    val = await WaitForValueAsync(async () => await GetRehabCount(store, databaseName), 1);
                    Assert.Equal(1, val);
                    val = await WaitForValueAsync(async () => await GetMembersCount(store, databaseName), clusterSize - 1);
                    Assert.Equal(clusterSize - 1, val);
                }

                val = await WaitForValueAsync(async () => await GetRehabCount(store, databaseName), 0);
                Assert.Equal(0, val);
                val = await WaitForValueAsync(async () => await GetMembersCount(store, databaseName), 2);
                Assert.Equal(clusterSize, val);
            }
        }

        [Fact]
        public async Task MoveLoadingNodeToLastAndRestoreToFixedOrder()
        {
            var clusterSize = 3;
            var settings = new Dictionary<string, string>()
            {
                [RavenConfiguration.GetKey(x => x.Cluster.ElectionTimeout)] = 300.ToString(),
                [RavenConfiguration.GetKey(x => x.Cluster.StabilizationTime)] = "1",
                [RavenConfiguration.GetKey(x => x.Cluster.MoveToRehabGraceTime)] = "10",
                [RavenConfiguration.GetKey(x => x.Cluster.RotatePreferredNodeGraceTime)] = "1",
                [RavenConfiguration.GetKey(x => x.Replication.ReplicationMinimalHeartbeat)] = "15",
            };

            var cluster = await CreateRaftCluster(clusterSize, false, 0, watcherCluster: true, customSettings: settings);
            using (var store = GetDocumentStore(new Options
            {
                Server = cluster.Leader,
                ReplicationFactor = clusterSize
            }))
            {
                var tcs = new TaskCompletionSource<DocumentDatabase>();

                var databaseName = store.Database;
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Karmel" }, "users/1");
                    session.SaveChanges();

                    Assert.True(await WaitForDocumentInClusterAsync<User>((DocumentSession)session, "users/1", _ => true, TimeSpan.FromSeconds(5)));
                }

                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                var fixedOrder = record.Topology.AllNodes.ToList();
                await store.Maintenance.Server.SendAsync(new ReorderDatabaseMembersOperation(store.Database, fixedOrder, fixedTopology: true));

                var preferred = Servers.Single(s => s.ServerStore.NodeTag == record.Topology.Members[0]);

                int val;
                using (new DisposableAction(() =>
                {
                    preferred.ServerStore.DatabasesLandlord.DatabasesCache.TryRemove(databaseName, out var t);
                    if (t == tcs.Task)
                        tcs.SetCanceled();
                }))
                {
                    var t = preferred.ServerStore.DatabasesLandlord.DatabasesCache.Replace(databaseName, tcs.Task);
                    t.Result.Dispose();

                    Assert.True(await WaitForValueAsync(async () =>
                    {
                        record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                        return record.Topology.Members[0] != preferred.ServerStore.NodeTag;
                    }, true));

                    val = await WaitForValueAsync(async () => await GetRehabCount(store, databaseName), 1);
                    Assert.Equal(1, val);
                    val = await WaitForValueAsync(async () => await GetMembersCount(store, databaseName), clusterSize - 1);
                    Assert.Equal(clusterSize - 1, val);
                }

                val = await WaitForValueAsync(async () => await GetRehabCount(store, databaseName), 0);
                Assert.Equal(0, val);
                val = await WaitForValueAsync(async () => await GetMembersCount(store, databaseName), 2);
                Assert.Equal(clusterSize, val);

                record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));

                Assert.Equal(fixedOrder, record.Topology.Members);
            }
        }

        [Fact]
        public async Task PromoteOnCatchingUp()
        {
            var clusterSize = 3;
            var databaseName = GetDatabaseName();
            var leader = await CreateRaftClusterAndGetLeader(clusterSize, true, 0);
            using (var store = new DocumentStore
            {
                Urls = new[] { leader.WebUrl },
                Database = databaseName
            }.Initialize())
            {
                var doc = new DatabaseRecord(databaseName);
                var createRes = await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(doc));

                var member = createRes.Topology.Members.Single();

                var dbServer = Servers.Single(s => s.ServerStore.NodeTag == member);
                await dbServer.ServerStore.Cluster.WaitForIndexNotification(createRes.RaftCommandIndex);

                await dbServer.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(databaseName);
                using (var dbStore = new DocumentStore
                {
                    Urls = new[] { dbServer.WebUrl },
                    Database = databaseName
                }.Initialize())
                {
                    using (var session = dbStore.OpenAsyncSession())
                    {
                        await session.StoreAsync(new User { Name = "Karmel" }, "users/1");
                        await session.SaveChangesAsync();
                    }
                }

                var res = await store.Maintenance.Server.SendAsync(new AddDatabaseNodeOperation(databaseName));
                Assert.Equal(1, res.Topology.Members.Count);
                Assert.Equal(1, res.Topology.Promotables.Count);

                await WaitForRaftIndexToBeAppliedInCluster(res.RaftCommandIndex, TimeSpan.FromSeconds(5));
                await WaitForDocumentInClusterAsync<User>(res.Topology, databaseName, "users/1", u => u.Name == "Karmel", TimeSpan.FromSeconds(10));
                await Task.Delay(TimeSpan.FromSeconds(5)); // wait for the observer
                var val = await WaitForValueAsync(async () => await GetPromotableCount(store, databaseName), 0);
                Assert.Equal(0, val);
                val = await WaitForValueAsync(async () => await GetMembersCount(store, databaseName), 2);
                Assert.Equal(2, val);
            }
        }

        [Fact]
        public async Task SuccessfulMaintenanceOnLeaderChange()
        {
            var clusterSize = 3;
            var databaseName = GetDatabaseName();
            var leader = await CreateRaftClusterAndGetLeader(clusterSize, true, 0);
            using (var store = new DocumentStore()
            {
                Urls = new[] { leader.WebUrl },
                Database = databaseName
            }.Initialize())
            {
                var doc = new DatabaseRecord(databaseName);
                var res = await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(doc, clusterSize));
                await WaitForRaftIndexToBeAppliedInCluster(res.RaftCommandIndex, TimeSpan.FromSeconds(5));
                Assert.Equal(3, res.Topology.Members.Count);
            }

            leader.Dispose();

            using (var store = new DocumentStore()
            {
                Urls = new[] { Servers[1].WebUrl },
                Database = databaseName
            }.Initialize())
            {
                var val = await WaitForValueAsync(async () => await GetMembersCount(store, databaseName), 2);
                Assert.Equal(2, val);
                val = await WaitForValueAsync(async () => await GetRehabCount(store, databaseName), 1);
                Assert.Equal(1, val);
            }
        }

        [Fact]
        public async Task PromoteDatabaseNodeBackAfterReconnection()
        {
            var clusterSize = 3;
            var databaseName = GetDatabaseName();
            var leader = await CreateRaftClusterAndGetLeader(clusterSize, false, 0, customSettings: new Dictionary<string, string>
            {
                [RavenConfiguration.GetKey(x => x.Cluster.MoveToRehabGraceTime)] = "4"
            });
            using (var store = new DocumentStore
            {
                Urls = new[] { leader.WebUrl },
                Database = databaseName
            }.Initialize())
            {
                var doc = new DatabaseRecord(databaseName);
                var databaseResult = await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(doc, clusterSize));
                Assert.Equal(clusterSize, databaseResult.Topology.Members.Count);
                await WaitForRaftIndexToBeAppliedInCluster(databaseResult.RaftCommandIndex, TimeSpan.FromSeconds(10));
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User());
                    await session.SaveChangesAsync();
                }

                var result = DisposeServerAndWaitForFinishOfDisposal(Servers[1]);

                var val = await WaitForValueAsync(async () => await GetMembersCount(store, databaseName), clusterSize - 1);
                Assert.Equal(clusterSize - 1, val);
                val = await WaitForValueAsync(async () => await GetRehabCount(store, databaseName), 1);
                Assert.Equal(1, val);
                WaitForUserToContinueTheTest(result.Url);
                Servers[1] = GetNewServer(
                    new ServerCreationOptions
                    {
                        CustomSettings = new Dictionary<string, string> { { RavenConfiguration.GetKey(x => x.Core.ServerUrls), result.Url } },
                        RunInMemory = false,
                        DeletePrevious = false,
                        DataDirectory = result.DataDirectory
                    });
                val = await WaitForValueAsync(async () => await GetMembersCount(store, databaseName), 3, 30_000);
                Assert.Equal(3, val);
                val = await WaitForValueAsync(async () => await GetRehabCount(store, databaseName), 0, 30_000);
                Assert.Equal(0, val);
            }
        }

        [Fact]
        public async Task MoveToPassiveWhenRefusedConnectionFromAllNodes()
        {
            //DebuggerAttachedTimeout.DisableLongTimespan = true;
            var clusterSize = 3;
            var databaseName = GetDatabaseName();
            var leader = await CreateRaftClusterAndGetLeader(clusterSize, false, 0, customSettings: new Dictionary<string, string>()
            {
                [RavenConfiguration.GetKey(x => x.Cluster.ElectionTimeout)] = "600"
            });

            using (var store = new DocumentStore
            {
                Urls = new[] { leader.WebUrl },
                Database = databaseName
            }.Initialize())
            {
                var doc = new DatabaseRecord(databaseName);
                var databaseResult = await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(doc, clusterSize));
                Assert.Equal(clusterSize, databaseResult.Topology.Members.Count);
                await WaitForRaftIndexToBeAppliedInCluster(databaseResult.RaftCommandIndex, TimeSpan.FromSeconds(10));
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User());
                    await session.SaveChangesAsync();
                }

                // kill the process and remove the node from topology
                var result = DisposeServerAndWaitForFinishOfDisposal(Servers[1]);

                await ActionWithLeader((l) => l.ServerStore.RemoveFromClusterAsync(result.NodeTag));

                using (leader.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                {
                    var val = await WaitForValueAsync(() =>
                    {
                        using (context.OpenReadTransaction())
                        {
                            return Servers[2].ServerStore.GetClusterTopology(context).AllNodes.Count;
                        }
                    }, clusterSize - 1);
                    Assert.Equal(clusterSize - 1, val);
                    val = await WaitForValueAsync(() =>
                    {
                        using (context.OpenReadTransaction())
                        {
                            return Servers[0].ServerStore.GetClusterTopology(context).AllNodes.Count;
                        }
                    }, clusterSize - 1);
                    Assert.Equal(clusterSize - 1, val);
                }
                // bring the node back to live and ensure that he moves to passive state
                Servers[1] = GetNewServer(
                    new ServerCreationOptions
                    {
                        CustomSettings = new Dictionary<string, string>
                        {
                            {RavenConfiguration.GetKey(x => x.Core.PublicServerUrl), result.Url},
                            {RavenConfiguration.GetKey(x => x.Core.ServerUrls), result.Url},
                            {RavenConfiguration.GetKey(x => x.Cluster.ElectionTimeout), "600"}
                        },
                        RunInMemory = false,
                        DeletePrevious = false,
                        DataDirectory = result.DataDirectory
                    });

                Assert.True(await Servers[1].ServerStore.WaitForState(RachisState.Passive, CancellationToken.None).WaitAsync(TimeSpan.FromSeconds(30)), "1st assert");
                // rejoin the node to the cluster

                await ActionWithLeader((l) => l.ServerStore.AddNodeToClusterAsync(result.Url, result.NodeTag));

                Assert.True(await Servers[1].ServerStore.WaitForState(RachisState.Follower, CancellationToken.None).WaitAsync(TimeSpan.FromSeconds(30)), "2nd assert");
            }
        }

        [Fact]
        public async Task RedistributeDatabaseIfNodeFails()
        {
            DebuggerAttachedTimeout.DisableLongTimespan = true;
            var clusterSize = 3;
            var dbGroupSize = 2;
            var databaseName = GetDatabaseName();
            var leader = await CreateRaftClusterAndGetLeader(clusterSize, false, 0);

            using (var store = new DocumentStore
            {
                Urls = new[] { leader.WebUrl },
                Database = databaseName
            }.Initialize())
            {
                var doc = new DatabaseRecord(databaseName)
                {
                    Topology = new DatabaseTopology
                    {
                        DynamicNodesDistribution = true
                    }
                };
                doc.Topology.Members.Add("A");
                doc.Topology.Members.Add("B");
                var databaseResult = await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(doc, dbGroupSize));
                Assert.Equal(dbGroupSize, databaseResult.Topology.Members.Count);
                await WaitForRaftIndexToBeAppliedInCluster(databaseResult.RaftCommandIndex, TimeSpan.FromSeconds(10));

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Karmel" }, "users/1");
                    await session.SaveChangesAsync();
                }
                Assert.True(await WaitForDocumentInClusterAsync<User>(doc.Topology, databaseName, "users/1", u => u.Name == "Karmel", TimeSpan.FromSeconds(5)));
                DisposeServerAndWaitForFinishOfDisposal(Servers[1]);

                // the db should move from node B to node C
                var newTopology = new DatabaseTopology();
                newTopology.Members.Add("A");
                newTopology.Members.Add("C");
                Assert.True(await WaitForDocumentInClusterAsync<User>(newTopology, databaseName, "users/1", u => u.Name == "Karmel", TimeSpan.FromSeconds(60)));
                var members = await WaitForValueAsync(async () => await GetMembersCount(store, databaseName), 2, 30_000);
                Assert.Equal(2, members);
            }
        }

        [Fact]
        public async Task RedistributeDatabaseOnMultiFailure()
        {
            DebuggerAttachedTimeout.DisableLongTimespan = true;
            var clusterSize = 5;
            var dbGroupSize = 3;
            var databaseName = GetDatabaseName();
            var leader = await CreateRaftClusterAndGetLeader(clusterSize, false, 0);

            using (var store = new DocumentStore
            {
                Urls = new[] { leader.WebUrl },
                Database = databaseName
            }.Initialize())
            {
                var doc = new DatabaseRecord(databaseName)
                {
                    Topology = new DatabaseTopology
                    {
                        DynamicNodesDistribution = true
                    }
                };
                doc.Topology.Members.Add("A");
                doc.Topology.Members.Add("B");
                doc.Topology.Members.Add("C");
                var databaseResult = await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(doc, dbGroupSize));
                Assert.Equal(dbGroupSize, databaseResult.Topology.Members.Count);
                await WaitForRaftIndexToBeAppliedInCluster(databaseResult.RaftCommandIndex, TimeSpan.FromSeconds(10));
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Karmel" }, "users/1");
                    await session.SaveChangesAsync();
                }
                Assert.True(await WaitForDocumentInClusterAsync<User>(doc.Topology, databaseName, "users/1", u => u.Name == "Karmel", TimeSpan.FromSeconds(5)));
                DisposeServerAndWaitForFinishOfDisposal(Servers[1]);
                DisposeServerAndWaitForFinishOfDisposal(Servers[2]);

                // the db should move to D & E
                var newTopology = new DatabaseTopology();
                newTopology.Members.Add("A");
                newTopology.Members.Add("D");
                newTopology.Members.Add("E");
                Assert.True(await WaitForDocumentInClusterAsync<User>(newTopology, databaseName, "users/1", u => u.Name == "Karmel", TimeSpan.FromSeconds(60)));
                var members = await WaitForValueAsync(async () => await GetMembersCount(store, databaseName), 3, 30_000);
                Assert.Equal(3, members);
            }
        }

        [Fact]
        public async Task RemoveNodeFromClusterWhileDeletion()
        {
            var databaseName = GetDatabaseName();
            var leader = await CreateRaftClusterAndGetLeader(3, leaderIndex: 0);

            using (var leaderStore = new DocumentStore
            {
                Urls = new[] { leader.WebUrl },
                Database = databaseName,
            })
            {
                leaderStore.Initialize();

                var (index, dbGroupNodes) = await CreateDatabaseInCluster(databaseName, 3, leader.WebUrl);
                await WaitForRaftIndexToBeAppliedInCluster(index, TimeSpan.FromSeconds(30));
                var dbToplogy = (await leaderStore.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(databaseName))).Topology;

                Assert.Equal(3, dbToplogy.Count);
                Assert.Equal(0, dbToplogy.Promotables.Count);

                var node = Servers[1].ServerStore.Engine.Tag;
                DisposeServerAndWaitForFinishOfDisposal(Servers[1]);
                var res = await leaderStore.Maintenance.Server.SendAsync(new DeleteDatabasesOperation(databaseName, true));

                Assert.Equal(1, await WaitForValueAsync(async () =>
                {
                    var records = await leaderStore.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(databaseName));
                    return records.DeletionInProgress.Count;
                }, 1));

                DatabaseRecord record = await leaderStore.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(databaseName));
                Assert.Single(record.DeletionInProgress);
                Assert.Equal(node, record.DeletionInProgress.First().Key);
                await leader.ServerStore.RemoveFromClusterAsync(node);

                await leader.ServerStore.WaitForCommitIndexChange(RachisConsensus.CommitIndexModification.GreaterOrEqual, res.RaftCommandIndex + 1);
                record = await leaderStore.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(databaseName));

                Assert.Null(record);
            }
        }

        [Fact]
        public async Task DontRemoveNodeWhileItHasNotReplicatedDocs()
        {
            DebuggerAttachedTimeout.DisableLongTimespan = true;

            var databaseName = GetDatabaseName();
            var settings = new Dictionary<string, string>
            {
                [RavenConfiguration.GetKey(x => x.Cluster.MoveToRehabGraceTime)] = "1",
                [RavenConfiguration.GetKey(x => x.Cluster.StabilizationTime)] = "1",
                [RavenConfiguration.GetKey(x => x.Cluster.AddReplicaTimeout)] = "1"
            };
            var leader = await CreateRaftClusterAndGetLeader(3, shouldRunInMemory: false, customSettings: settings);

            using (var leaderStore = new DocumentStore
            {
                Urls = new[] { leader.WebUrl },
                Database = databaseName,
            })
            {
                leaderStore.Initialize();
                var topology = new DatabaseTopology
                {
                    Members = new List<string>
                    {
                        "B",
                        "C"
                    },
                    DynamicNodesDistribution = true
                };
                var (index, dbGroupNodes) = await CreateDatabaseInCluster(new DatabaseRecord
                {
                    DatabaseName = databaseName,
                    Topology = topology
                }, 2, leader.WebUrl);
                await WaitForRaftIndexToBeAppliedInCluster(index, TimeSpan.FromSeconds(30));

                using (var session = leaderStore.OpenSession())
                {
                    session.Store(new User(), "users/1");
                    session.SaveChanges();
                }
                var dbToplogy = (await leaderStore.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(databaseName))).Topology;
                Assert.Equal(2, dbToplogy.AllNodes.Count());
                Assert.Equal(0, dbToplogy.Promotables.Count);
                Assert.True(await WaitForDocumentInClusterAsync<User>(topology, databaseName, "users/1", null, TimeSpan.FromSeconds(30)));

                var serverA = Servers.Single(s => s.ServerStore.NodeTag == "A");
                var resultA = DisposeServerAndWaitForFinishOfDisposal(serverA);

                var serverB = Servers.Single(s => s.ServerStore.NodeTag == "B");
                var resultB = DisposeServerAndWaitForFinishOfDisposal(serverB);

                // write doc only to C
                using (var session = leaderStore.OpenSession())
                {
                    session.Store(new User(), "users/2");
                    session.SaveChanges();
                }

                var serverC = Servers.Single(s => s.ServerStore.NodeTag == "C");
                var resultC = DisposeServerAndWaitForFinishOfDisposal(serverC);

                settings[RavenConfiguration.GetKey(x => x.Core.ServerUrls)] = resultA.Url;
                Servers[0] = GetNewServer(
                    new ServerCreationOptions
                    {
                        CustomSettings = settings,
                        RunInMemory = false,
                        DeletePrevious = false,
                        DataDirectory = resultA.DataDirectory
                    });

                settings[RavenConfiguration.GetKey(x => x.Core.ServerUrls)] = resultB.Url;
                Servers[1] = GetNewServer(new ServerCreationOptions
                {
                    CustomSettings = settings,
                    RunInMemory = false,
                    DeletePrevious = false,
                    DataDirectory = resultB.DataDirectory
                });
                await Task.Delay(TimeSpan.FromSeconds(10));
                Assert.Equal(2, await WaitForValueAsync(async () => await GetMembersCount(leaderStore, databaseName), 2));
                Assert.Equal(1, await WaitForValueAsync(async () => await GetRehabCount(leaderStore, databaseName), 1));
                Assert.Equal(1, await WaitForValueAsync(async () => await GetDeletionCount(leaderStore, databaseName), 1));

                using (var session = leaderStore.OpenSession())
                {
                    session.Store(new User(), "users/3");
                    session.SaveChanges();
                }
                Assert.True(await WaitForDocumentInClusterAsync<User>(new DatabaseTopology
                {
                    Members = new List<string> { "A", "B" }
                }, databaseName, "users/3", null, TimeSpan.FromSeconds(10)));

                settings[RavenConfiguration.GetKey(x => x.Core.ServerUrls)] = resultC.Url;
                var mre = new ManualResetEventSlim(false);
                Servers[2] = GetNewServer(new ServerCreationOptions
                {
                    CustomSettings = settings,
                    RunInMemory = false,
                    DeletePrevious = false,
                    DataDirectory = resultC.DataDirectory,
                    BeforeDatabasesStartup = (server) =>
                    {
                        while (server.LoadDatabaseTopology(databaseName).Rehabs.Contains("C") == false)
                        {
                            Thread.Sleep(100);
                        }
                        mre.Set();
                    }
                });

                if (mre.Wait(TimeSpan.FromSeconds(30)) == false)
                    throw new TimeoutException();

                Assert.Equal(2, await WaitForValueAsync(async () => await GetMembersCount(leaderStore, databaseName), 2));
                Assert.Equal(0, await WaitForValueAsync(async () => await GetRehabCount(leaderStore, databaseName), 0, 30_000));

                dbToplogy = (await leaderStore.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(databaseName))).Topology;
                Assert.True(await WaitForDocumentInClusterAsync<User>(dbToplogy, databaseName, "users/3", null, TimeSpan.FromSeconds(10)));

                dbToplogy = (await leaderStore.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(databaseName))).Topology;
                Assert.Equal(2, dbToplogy.AllNodes.Count());
                Assert.Equal(2, dbToplogy.Members.Count);
                Assert.Equal(0, dbToplogy.Rehabs.Count);

                Assert.True(await WaitForDocumentInClusterAsync<User>(dbToplogy, databaseName, "users/1", null, TimeSpan.FromSeconds(10)));
                Assert.True(await WaitForDocumentInClusterAsync<User>(dbToplogy, databaseName, "users/3", null, TimeSpan.FromSeconds(10)));
                Assert.True(await WaitForDocumentInClusterAsync<User>(dbToplogy, databaseName, "users/2", null, TimeSpan.FromSeconds(30)));

                dbToplogy = (await leaderStore.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(databaseName))).Topology;
                Assert.Equal(2, dbToplogy.AllNodes.Count());
                Assert.Equal(2, dbToplogy.Members.Count);
                Assert.Equal(0, dbToplogy.Rehabs.Count);
            }
        }

        [Fact]
        public async Task Promote_immedtialty_should_work()
        {
            var databaseName = GetDatabaseName();
            var leader = await CreateRaftClusterAndGetLeader(3);

            using (var leaderStore = new DocumentStore
            {
                Urls = new[] { leader.WebUrl },
                Database = databaseName,
            })
            {
                leaderStore.Initialize();

                var (index, dbGroupNodes) = await CreateDatabaseInCluster(databaseName, 2, leader.WebUrl);
                await WaitForRaftIndexToBeAppliedInCluster(index, TimeSpan.FromSeconds(30));
                var dbToplogy = (await leaderStore.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(databaseName))).Topology;

                Assert.Equal(2, dbToplogy.AllNodes.Count());
                Assert.Equal(0, dbToplogy.Promotables.Count);

                var nodeNotInDbGroup = Servers.Single(s => dbGroupNodes.Contains(s) == false)?.ServerStore.NodeTag;
                leaderStore.Maintenance.Server.Send(new AddDatabaseNodeOperation(databaseName, nodeNotInDbGroup));
                dbToplogy = (await leaderStore.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(databaseName))).Topology;
                Assert.Equal(3, dbToplogy.AllNodes.Count());
                Assert.Equal(1, dbToplogy.Promotables.Count);
                Assert.Equal(nodeNotInDbGroup, dbToplogy.Promotables[0]);

                await leaderStore.Maintenance.Server.SendAsync(new PromoteDatabaseNodeOperation(databaseName, nodeNotInDbGroup));
                dbToplogy = (await leaderStore.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(databaseName))).Topology;

                Assert.Equal(3, dbToplogy.AllNodes.Count());
                Assert.Equal(0, dbToplogy.Promotables.Count);
            }
        }

        [Fact]
        public async Task ChangeUrlOfSingleNodeCluster()
        {
            var databaseName = GetDatabaseName();
            var leader = await CreateRaftClusterAndGetLeader(1, shouldRunInMemory: false);

            using (var leaderStore = new DocumentStore
            {
                Urls = new[] { leader.WebUrl },
                Database = databaseName,
            })
            {
                leaderStore.Initialize();
                await CreateDatabaseInCluster(databaseName, 1, leader.WebUrl);
                var dbToplogy = (await leaderStore.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(databaseName))).Topology;
                Assert.Equal(1, dbToplogy.Members.Count);
            }
            var result = DisposeServerAndWaitForFinishOfDisposal(Servers[0]);
            var customSettings = new Dictionary<string, string>();
            var certificates = SetupServerAuthentication(customSettings);
            customSettings[RavenConfiguration.GetKey(x => x.Core.ServerUrls)] = "https://" + Environment.MachineName + ":8999";
            leader = Servers[0] = GetNewServer(new ServerCreationOptions
            {
                CustomSettings = customSettings,
                RunInMemory = false,
                DeletePrevious = false,
                DataDirectory = result.DataDirectory
            });

            var adminCert = RegisterClientCertificate(certificates, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin, server: leader);

            using (var leaderStore = new DocumentStore
            {
                Certificate = adminCert,
                Urls = new[] { leader.WebUrl },
                Database = databaseName,
            })
            {
                leaderStore.Initialize();
                await Task.Delay(TimeSpan.FromSeconds(5)); // wait for the observer to update the status
                var dbToplogy = (await leaderStore.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(databaseName))).Topology;
                Assert.Equal(1, dbToplogy.Members.Count);
            }
        }

        [Fact]
        public async Task ChangeUrlOfMultiNodeCluster()
        {
            var fromSeconds = TimeSpan.FromSeconds(8);

            var databaseName = GetDatabaseName();
            var groupSize = 3;
            var newUrl = "http://127.0.0.1:0";
            string nodeTag;

            var leader = await CreateRaftClusterAndGetLeader(groupSize, shouldRunInMemory: false, leaderIndex: 0, customSettings: new Dictionary<string, string>
            {
                [RavenConfiguration.GetKey(x => x.Cluster.MoveToRehabGraceTime)] = "4"
            });

            using (var leaderStore = new DocumentStore
            {
                Urls = new[] { leader.WebUrl },
                Database = databaseName
            })
            {
                leaderStore.Initialize();
                await CreateDatabaseInCluster(databaseName, groupSize, leader.WebUrl);

                var dbToplogy = (await leaderStore.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(databaseName))).Topology;
                Assert.Equal(groupSize, dbToplogy.Members.Count);

                // kill and change the url
                var result = DisposeServerAndWaitForFinishOfDisposal(Servers[1]);
                nodeTag = result.NodeTag;

                var customSettings = new Dictionary<string, string>
                {
                    [RavenConfiguration.GetKey(x => x.Core.ServerUrls)] = newUrl,
                    [RavenConfiguration.GetKey(x => x.Security.UnsecuredAccessAllowed)] = UnsecuredAccessAddressRange.PublicNetwork.ToString()
                };
                Servers[1] = GetNewServer(new ServerCreationOptions
                {
                    CustomSettings = customSettings,
                    RunInMemory = false,
                    DeletePrevious = false,
                    DataDirectory = result.DataDirectory
                });
                newUrl = Servers[1].WebUrl;
                // ensure that at this point we still can't talk to node
                await Task.Delay(fromSeconds); // wait for the observer to update the status
                dbToplogy = (await leaderStore.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(databaseName))).Topology;
                Assert.Equal(1, dbToplogy.Rehabs.Count);
                Assert.Equal(groupSize - 1, dbToplogy.Members.Count);
            }

            await WaitForLeader(fromSeconds);
            leader = Servers.Single(s => s.Disposed == false && s.ServerStore.IsLeader());

            // remove and rejoin to change the url
            Assert.True(await leader.ServerStore.RemoveFromClusterAsync(nodeTag).WaitAsync(fromSeconds));
            Assert.True(await Servers[1].ServerStore.WaitForState(RachisState.Passive, CancellationToken.None).WaitAsync(fromSeconds));

            Assert.True(await leader.ServerStore.AddNodeToClusterAsync(Servers[1].ServerStore.GetNodeHttpServerUrl(), nodeTag).WaitAsync(fromSeconds));
            Assert.True(await Servers[1].ServerStore.WaitForState(RachisState.Follower, CancellationToken.None).WaitAsync(fromSeconds));

            Assert.Equal(3, WaitForValue(() => leader.ServerStore.GetClusterTopology().Members.Count, 3));

            // create a new database and verify that it resides on the server with the new url
            var (_, dbGroupNodes) = await CreateDatabaseInCluster(GetDatabaseName(), groupSize, leader.WebUrl);
            Assert.True(dbGroupNodes.Select(s => s.WebUrl).Contains(newUrl));
        }

        [Fact]
        public async Task RavenDB_12744()
        {
            var databaseName = GetDatabaseName();
            var leader = await CreateRaftClusterAndGetLeader(3);
            var result = await CreateDatabaseInCluster(databaseName, 1, leader.WebUrl);

            using (var store = new DocumentStore
            {
                Database = databaseName,
                Urls = new[] { leader.WebUrl }
            }.Initialize())
            {
                using (var commands = store.Commands())
                {
                    await commands.PutAsync("users/1", null, new Raven.Tests.Core.Utils.Entities.User { Name = "Fitzchak" });
                }

                using (var a1 = new MemoryStream(new byte[] { 1, 2, 3 }))
                {
                    await store.Operations.SendAsync(new PutAttachmentOperation("users/1", "a1", a1, "a1/png"));
                }

                var res = await store.Maintenance.Server.SendAsync(new AddDatabaseNodeOperation(databaseName));
                var val = await WaitForValueAsync(async () => await GetMembersCount(store, databaseName), 2);
                Assert.Equal(2, val);

                res = await store.Maintenance.Server.SendAsync(new AddDatabaseNodeOperation(databaseName));
                val = await WaitForValueAsync(async () => await GetMembersCount(store, databaseName), 3);
                Assert.Equal(3, val);
            }
        }

        [Fact]
        public async Task OutOfCpuCreditShouldMoveToRehab()
        {
            var cluster = await CreateRaftCluster(3);

            using (var store = GetDocumentStore(new Options
            {
                Server = cluster.Leader,
                ReplicationFactor = 3
            }))
            {
                cluster.Nodes[0].CpuCreditsBalance.BackgroundTasksAlertRaised.Raise();
                var rehabs = await WaitForValueAsync(async () => await GetRehabCount(store, store.Database), 1);
                Assert.Equal(1, rehabs);

                cluster.Nodes[0].CpuCreditsBalance.BackgroundTasksAlertRaised.Lower();
                var members = await WaitForValueAsync(async () => await GetMembersCount(store, store.Database), 3);
                Assert.Equal(3, members);
            }
        }

        [Fact]
        public async Task ReduceChangeVectorWhenRemovingNode()
        {
            var cluster = await CreateRaftCluster(3);

            using (var store = GetDocumentStore(new Options
            {
                Server = cluster.Leader,
                ReplicationFactor = 3
            }))
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User(), "foo.bar");
                    await session.SaveChangesAsync();
                }

                await WaitForDocumentInClusterAsync<User>(store.GetRequestExecutor().TopologyNodes, "foo.bar", null, TimeSpan.FromSeconds(10));

                // it takes a heartbeat to update the sibling change vector
                await Task.Delay(2 * cluster.Leader.ServerStore.Configuration.Replication.ReplicationMinimalHeartbeat.AsTimeSpan);

                using (var session = store.OpenAsyncSession())
                {
                    var user = new User();
                    await session.StoreAsync(user, "foo.bar.2");
                    await session.SaveChangesAsync();
                    Assert.Equal(3, session.Advanced.GetChangeVectorFor(user).ToChangeVectorList().Count);
                }

                var deleteResult = await store.Maintenance.Server.SendAsync(new DeleteDatabasesOperation(store.Database, true, "A"));
                await WaitForRaftIndexToBeAppliedInCluster(deleteResult.RaftCommandIndex, TimeSpan.FromSeconds(10));
                var db = store.Database;

                Func<ServerStore, bool> waitFunc = (s) =>
                {
                    using (s.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                    using (ctx.OpenReadTransaction())
                    {
                        var result = s.Cluster.ReadDatabase(ctx, db);
                        return result.DeletionInProgress.Count == 0 && result.Topology.AllNodes.Count() == 2;
                    }
                };

                Assert.True(await WaitForValueOnGroupAsync(new DatabaseTopology {Members = new List<string> {"A", "B", "C"}}, waitFunc, true));

                using (var session = store.OpenAsyncSession())
                {
                    var user = new User();
                    await session.StoreAsync(user, "foo.bar.3");
                    await session.SaveChangesAsync();
                    Assert.Equal(2, session.Advanced.GetChangeVectorFor(user).ToChangeVectorList().Count);
                }
            }
        }

        [Fact]
        public async Task CanRemoveChangeVector()
        {
            using (var store = GetDocumentStore())
            {
                await store.Maintenance.SendAsync(new CreateSampleDataOperation());

                await store.Maintenance.Server.SendAsync(new UpdateUnusedDatabasesOperation(store.Database, new HashSet<string>
                {
                    "xwmnvG1KBkSNXfl7/0yJ1A",
                    "0N64iiIdYUKcO+yq1V0cPA"
                }));

                await WaitForRaftCommandToBeAppliedInLocalServer(nameof(UpdateUnusedDatabaseIdsCommand));

                using (var session = store.OpenAsyncSession())
                {
                    var user = new User();
                    await session.StoreAsync(user, "foo/bar");
                    await session.SaveChangesAsync();
                    Assert.Equal(1, session.Advanced.GetChangeVectorFor(user).ToChangeVectorList().Count);
                }

                await store.Maintenance.Server.SendAsync(new UpdateUnusedDatabasesOperation(store.Database, null));
                await WaitForRaftCommandToBeAppliedInLocalServer(nameof(UpdateUnusedDatabaseIdsCommand));

                using (var session = store.OpenAsyncSession())
                {
                    var user = new User();
                    await session.StoreAsync(user, "foo/bar/2");
                    await session.SaveChangesAsync();
                    Assert.Equal(1, session.Advanced.GetChangeVectorFor(user).ToChangeVectorList().Count);
                }
            }
        }

        [Fact]
        public async Task HandleConflictShouldTakeUnusedDatabasesIntoAccount()
        {
            var database = GetDatabaseName();
            var cluster = await CreateRaftCluster(3);

            await CreateDatabaseInCluster(database, 3, cluster.Leader.WebUrl);

            using var store1 = new DocumentStore
            {
                Database = database,
                Urls = new[] { cluster.Nodes[0].WebUrl },
                Conventions = new DocumentConventions
                {
                    DisableTopologyUpdates = true
                }
            }.Initialize();

            using var store2 = new DocumentStore
            {
                Database = database,
                Urls = new[] { cluster.Nodes[1].WebUrl },
                Conventions = new DocumentConventions
                {
                    DisableTopologyUpdates = true
                }
            }.Initialize();

            using var store3 = new DocumentStore
            {
                Database = database,
                Urls = new[] { cluster.Nodes[2].WebUrl },
                Conventions = new DocumentConventions
                {
                    DisableTopologyUpdates = true
                }
            }.Initialize();

            using (var session = store1.OpenAsyncSession())
            {
                session.Advanced.WaitForReplicationAfterSaveChanges(replicas: 2);

                await session.StoreAsync(new User(), "foo/bar");
                await session.SaveChangesAsync();
            }

            using (var session = store2.OpenAsyncSession())
            {
                session.Advanced.WaitForReplicationAfterSaveChanges(replicas: 2);

                var user = await session.LoadAsync<User>("foo/bar");
                user.Name = "Karmel";
                await session.SaveChangesAsync();
            }

            await store2.Maintenance.Server.SendAsync(new DeleteDatabasesOperation(database, hardDelete: true, fromNode: cluster.Nodes[1].ServerStore.NodeTag, timeToWaitForConfirmation: TimeSpan.FromSeconds(15)));

            await Task.Delay(3000);

            using (var session = store3.OpenAsyncSession())
            {
                var user = await session.LoadAsync<User>("foo/bar");
                session.Advanced.WaitForReplicationAfterSaveChanges();

                using (var stream = new MemoryStream(new byte[] { 1, 2, 3, 4, 5 }))
                {
                    session.Advanced.Attachments.Store(user, "dummy", stream);
                    user.Name = "Oops";
                    await session.SaveChangesAsync();
                }
            }

            using (var session = store1.OpenAsyncSession())
            {
                var rev = await session.Advanced.Revisions.GetMetadataForAsync("foo/bar");
                Assert.Equal(0, rev.Count);
            }
        }

        [Fact]
        public async Task HandleConflictShouldTakeUnusedDatabasesIntoAccount2()
        {
            var database = GetDatabaseName();
            var cluster = await CreateRaftCluster(3, watcherCluster: true);
            var databaseResult = await CreateDatabaseInCluster(database, 3, cluster.Leader.WebUrl);

            using var store1 = new DocumentStore
            {
                Database = database,
                Urls = new[] { cluster.Nodes[0].WebUrl },
                Conventions = new DocumentConventions
                {
                    DisableTopologyUpdates = true
                }
            }.Initialize();

            using var store2 = new DocumentStore
            {
                Database = database,
                Urls = new[] { cluster.Nodes[1].WebUrl },
                Conventions = new DocumentConventions
                {
                    DisableTopologyUpdates = true
                }
            }.Initialize();

            using var store3 = new DocumentStore
            {
                Database = database,
                Urls = new[] { cluster.Nodes[2].WebUrl },
                Conventions = new DocumentConventions
                {
                    DisableTopologyUpdates = true
                }
            }.Initialize();
            
            
            var allStores = new [] {(DocumentStore)store1, (DocumentStore)store2, (DocumentStore)store3};
            var toDelete = cluster.Nodes.First(n => n != cluster.Leader);
            var toBeDeletedStore = allStores.Single(s => s.Urls[0] == toDelete.WebUrl);
            var nonDeletedStores = allStores.Where(s => s.Urls[0] != toDelete.WebUrl).ToArray();
            var nonDeletedNodes = cluster.Nodes.Where(n => n.ServerStore.NodeTag != toDelete.ServerStore.NodeTag).ToArray();
            var deletedNode = cluster.Nodes.Single(n => n.ServerStore.NodeTag == toDelete.ServerStore.NodeTag);

            var deletedStorage = await deletedNode.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
            var mre = new ManualResetEventSlim(false);
            deletedStorage.ReplicationLoader.DebugWaitAndRunReplicationOnce = mre;

            var nonDeletedStorage1 = await nonDeletedNodes[0].ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
            var mre1 = new ManualResetEventSlim(false);
            nonDeletedStorage1.ReplicationLoader.DebugWaitAndRunReplicationOnce = mre1;

            var nonDeletedStorage2 = await nonDeletedNodes[1].ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
            var mre2 = new ManualResetEventSlim(false);
            nonDeletedStorage2.ReplicationLoader.DebugWaitAndRunReplicationOnce = mre2;

            using (var session = nonDeletedStores[0].OpenAsyncSession())
            {
                await session.StoreAsync(new User {Name = "Karmel"}, "foo/bar");
                await session.SaveChangesAsync();
            }

            using (var session = toBeDeletedStore.OpenAsyncSession())
            {
                await session.StoreAsync(new User{Name = "Karmel2"}, "foo/bar");
                await session.SaveChangesAsync();
            }

            var t1 = Task.Run(()=>WaitForDocument<User>(nonDeletedStores[0], "foo/bar", u => u.Name == "Karmel2"));
            var t2 = Task.Run(()=>WaitForDocument<User>(nonDeletedStores[1], "foo/bar", u => u.Name == "Karmel2"));

            var t = Task.WhenAll(t1, t2);
            while (t.IsCompleted == false)
            {
                mre.Set();
                await Task.Delay(250);
            }

            Assert.True(await t1);
            Assert.True(await t2);
            
            
            using (var session1 = nonDeletedStores[0].OpenAsyncSession())
            {
                var rev1 = await session1.Advanced.Revisions.GetMetadataForAsync("foo/bar");
                Assert.Equal(3, rev1.Count);
            }

            var deleteResult = await nonDeletedStores[0].Maintenance.Server.SendAsync(new DeleteDatabasesOperation(database, hardDelete: true, fromNode: toDelete.ServerStore.NodeTag, timeToWaitForConfirmation: TimeSpan.FromSeconds(15)));

            await Task.WhenAll(nonDeletedNodes.Select(n =>
                n.ServerStore.WaitForCommitIndexChange(RachisConsensus.CommitIndexModification.GreaterOrEqual, deleteResult.RaftCommandIndex + 1)));

            var record = await nonDeletedStores[0].Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(database));
            Assert.Equal(1, record.UnusedDatabaseIds.Count);

            nonDeletedStorage2.ReplicationLoader.DebugWaitAndRunReplicationOnce = null;
            nonDeletedStorage1.ReplicationLoader.DebugWaitAndRunReplicationOnce = null;

            mre1.Set();
            mre2.Set();

            await Task.Delay(1000);
            EnsureReplicating(nonDeletedStores[0], nonDeletedStores[1]);
            EnsureReplicating(nonDeletedStores[1], nonDeletedStores[0]);


            using (var session1 = nonDeletedStores[0].OpenAsyncSession())
            using (var session2 = nonDeletedStores[1].OpenAsyncSession())
            {
                var rev1 = await session1.Advanced.Revisions.GetMetadataForAsync("foo/bar");
                var rev2 = await session2.Advanced.Revisions.GetMetadataForAsync("foo/bar");
                Assert.Equal(rev2.Count, rev1.Count);
                Assert.Equal(3, rev1.Count);
            }
        }

        [Fact]
        public async Task HandleConflictShouldTakeUnusedDatabasesIntoAccount3()
        {
            var database = GetDatabaseName();
            var cluster = await CreateRaftCluster(3, watcherCluster: true);
            var databaseResult = await CreateDatabaseInCluster(database, 3, cluster.Leader.WebUrl);

            using var store1 = new DocumentStore
            {
                Database = database,
                Urls = new[] { cluster.Nodes[0].WebUrl },
                Conventions = new DocumentConventions
                {
                    DisableTopologyUpdates = true
                }
            }.Initialize();

            using var store2 = new DocumentStore
            {
                Database = database,
                Urls = new[] { cluster.Nodes[1].WebUrl },
                Conventions = new DocumentConventions
                {
                    DisableTopologyUpdates = true
                }
            }.Initialize();

            using var store3 = new DocumentStore
            {
                Database = database,
                Urls = new[] { cluster.Nodes[2].WebUrl },
                Conventions = new DocumentConventions
                {
                    DisableTopologyUpdates = true
                }
            }.Initialize();
            
            
            var allStores = new [] {(DocumentStore)store1, (DocumentStore)store2, (DocumentStore)store3};
            var toDelete = cluster.Nodes.First(n => n != cluster.Leader);
            var toBeDeletedStore = allStores.Single(s => s.Urls[0] == toDelete.WebUrl);
            var nonDeletedStores = allStores.Where(s => s.Urls[0] != toDelete.WebUrl).ToArray();
            var nonDeletedNodes = cluster.Nodes.Where(n => n.ServerStore.NodeTag != toDelete.ServerStore.NodeTag).ToArray();
            var deletedNode = cluster.Nodes.Single(n => n.ServerStore.NodeTag == toDelete.ServerStore.NodeTag);

            var deletedStorage = await deletedNode.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
            var mre = new ManualResetEventSlim(false);
            deletedStorage.ReplicationLoader.DebugWaitAndRunReplicationOnce = mre;

            var nonDeletedStorage1 = await nonDeletedNodes[0].ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
            var mre1 = new ManualResetEventSlim(false);
            nonDeletedStorage1.ReplicationLoader.DebugWaitAndRunReplicationOnce = mre1;

            var nonDeletedStorage2 = await nonDeletedNodes[1].ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
            var mre2 = new ManualResetEventSlim(false);
            nonDeletedStorage2.ReplicationLoader.DebugWaitAndRunReplicationOnce = mre2;

            using (var session = toBeDeletedStore.OpenAsyncSession())
            {
                await session.StoreAsync(new User{Name = "Karmel"}, "foo/bar");
                await session.SaveChangesAsync();
            }

            using (var session = toBeDeletedStore.OpenSession())
            {
                var t = WaitForDocumentInClusterAsync<User>((DocumentSession)session, "foo/bar", u => u.Name == "Karmel", TimeSpan.FromSeconds(15));

                while (t.IsCompleted == false)
                {
                    mre.Set();
                    await Task.Delay(250);
                }

                await t;
            }

            var deleteResult = await nonDeletedStores[0].Maintenance.Server.SendAsync(new DeleteDatabasesOperation(database, hardDelete: true, fromNode: toDelete.ServerStore.NodeTag, timeToWaitForConfirmation: TimeSpan.FromSeconds(15)));

            await Task.WhenAll(nonDeletedNodes.Select(n =>
                n.ServerStore.WaitForCommitIndexChange(RachisConsensus.CommitIndexModification.GreaterOrEqual, deleteResult.RaftCommandIndex + 1)));

            var record = await nonDeletedStores[0].Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(database));
            Assert.Equal(1, record.UnusedDatabaseIds.Count);

            nonDeletedStorage2.ReplicationLoader.DebugWaitAndRunReplicationOnce = null;
            nonDeletedStorage1.ReplicationLoader.DebugWaitAndRunReplicationOnce = null;

            mre1.Set();
            mre2.Set();

            await Task.Delay(1000);
            EnsureReplicating(nonDeletedStores[0], nonDeletedStores[1]);
            EnsureReplicating(nonDeletedStores[1], nonDeletedStores[0]);

            await EnsureNoReplicationLoop(nonDeletedNodes[0], database);
            await EnsureNoReplicationLoop(nonDeletedNodes[1], database);
        }

        [Fact]
        public async Task HandleConflictShouldTakeUnusedDatabasesIntoAccount4()
        {
            var database = GetDatabaseName();
            var cluster = await CreateRaftCluster(3, watcherCluster: true);
            var databaseResult = await CreateDatabaseInCluster(database, 3, cluster.Leader.WebUrl);

            using var store1 = new DocumentStore
            {
                Database = database,
                Urls = new[] { cluster.Nodes[0].WebUrl },
                Conventions = new DocumentConventions
                {
                    DisableTopologyUpdates = true
                }
            }.Initialize();

            using var store2 = new DocumentStore
            {
                Database = database,
                Urls = new[] { cluster.Nodes[1].WebUrl },
                Conventions = new DocumentConventions
                {
                    DisableTopologyUpdates = true
                }
            }.Initialize();

            using var store3 = new DocumentStore
            {
                Database = database,
                Urls = new[] { cluster.Nodes[2].WebUrl },
                Conventions = new DocumentConventions
                {
                    DisableTopologyUpdates = true
                }
            }.Initialize();
            
            
            var allStores = new [] {(DocumentStore)store1, (DocumentStore)store2, (DocumentStore)store3};
            var toDelete = cluster.Nodes.First(n => n != cluster.Leader);
            var toBeDeletedStore = allStores.Single(s => s.Urls[0] == toDelete.WebUrl);
            var nonDeletedStores = allStores.Where(s => s.Urls[0] != toDelete.WebUrl).ToArray();
            var nonDeletedNodes = cluster.Nodes.Where(n => n.ServerStore.NodeTag != toDelete.ServerStore.NodeTag).ToArray();

            await RevisionsHelper.SetupRevisions(toDelete.ServerStore, database, new RevisionsConfiguration
            {
                Default = new RevisionsCollectionConfiguration
                {
                    Disabled = false
                }
            });

            using (var session = toBeDeletedStore.OpenAsyncSession())
            {
                await session.StoreAsync(new User{Name = "Karmel"}, "foo/bar");
                await session.SaveChangesAsync();
            }

            using (var session = toBeDeletedStore.OpenAsyncSession())
            {
                session.Delete("foo/bar");
                await session.SaveChangesAsync();
            }

            using (var session = toBeDeletedStore.OpenAsyncSession())
            {
                await session.StoreAsync(new User{Name = "Karmel2"}, "foo/bar");
                await session.SaveChangesAsync();
            }

            using (var session = toBeDeletedStore.OpenSession())
            {
                var t = await WaitForDocumentInClusterAsync<User>((DocumentSession)session, "foo/bar", u => u.Name == "Karmel2", TimeSpan.FromSeconds(15));
                Assert.True(t);
            }

            var rep1 = await BreakReplication(nonDeletedNodes[0].ServerStore, database);
            var rep2 = await BreakReplication(nonDeletedNodes[1].ServerStore, database);

            await RevisionsHelper.SetupRevisions(nonDeletedNodes[0].ServerStore, database, new RevisionsConfiguration
            {
                Default = new RevisionsCollectionConfiguration
                {
                    Disabled = false,
                    MinimumRevisionsToKeep = 0
                }
            });

            using (var session = toBeDeletedStore.OpenAsyncSession())
            {
                await session.StoreAsync(new User{Name = "Karmel3"}, "foo/bar");
                await session.SaveChangesAsync();
            }

            using (var session = toBeDeletedStore.OpenSession())
            {
                var t = await WaitForDocumentInClusterAsync<User>((DocumentSession)session, "foo/bar", u => u.Name == "Karmel3", TimeSpan.FromSeconds(15));
                Assert.True(t);
            }

            await RemoveDatabaseNode(cluster.Nodes, database, toDelete.ServerStore.NodeTag);

            rep1.Mend();
            rep2.Mend();

            await Task.Delay(1000);
            EnsureReplicating(nonDeletedStores[0], nonDeletedStores[1]);
            EnsureReplicating(nonDeletedStores[1], nonDeletedStores[0]);

            await EnsureNoReplicationLoop(nonDeletedNodes[0], database);
            await EnsureNoReplicationLoop(nonDeletedNodes[1], database);
        }

        [Fact]
        public async Task KeepDatabaseIdOnSoftDelete()
        {
            var cluster = await CreateRaftCluster(3, watcherCluster: true);
            using (var store = GetDocumentStore(new Options
            {
                Server = cluster.Leader,
                ReplicationFactor = 3
            }))
            {
                var result = await store.Maintenance.Server.SendAsync(new DeleteDatabasesOperation(store.Database, hardDelete: true, "A", timeToWaitForConfirmation: TimeSpan.FromSeconds(15)));
                await WaitForRaftIndexToBeAppliedInCluster(result.RaftCommandIndex + 1, TimeSpan.FromSeconds(15));
                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                Assert.Equal(1, record.UnusedDatabaseIds.Count);

                result = await store.Maintenance.Server.SendAsync(new DeleteDatabasesOperation(store.Database, hardDelete: false, "B", timeToWaitForConfirmation: TimeSpan.FromSeconds(15)));
                await WaitForRaftIndexToBeAppliedInCluster(result.RaftCommandIndex + 1, TimeSpan.FromSeconds(15));
                record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                Assert.Equal(1, record.UnusedDatabaseIds.Count);
            }
        }

        [Fact]
        public async Task WaitBreakdownTimeBeforeReplacing()
        {
            var clusterSize = 3;
            var cluster = await CreateRaftCluster(clusterSize, true, 0, customSettings: new Dictionary<string, string>
            {
                [RavenConfiguration.GetKey(x => x.Cluster.MoveToRehabGraceTime)] = "1",
                [RavenConfiguration.GetKey(x => x.Cluster.StabilizationTime)] = "1",
                [RavenConfiguration.GetKey(x => x.Cluster.AddReplicaTimeout)] = "5"
            });
            using (var store = new DocumentStore
            {
                Urls = new[] { cluster.Leader.WebUrl },
            }.Initialize())
            {
                var name = GetDatabaseName();
                var doc = new DatabaseRecord(name)
                {
                    Topology = new DatabaseTopology
                    {
                        Members = new List<string>
                        {
                            "A",
                            "B"
                        },
                        ReplicationFactor = 2,
                        DynamicNodesDistribution = true
                    }
                };
                var databaseResult = await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(doc, 2));
                Assert.Equal(2, databaseResult.Topology.Members.Count);

                var node = cluster.Nodes.Single(n => n.ServerStore.NodeTag == "B");
                await DisposeServerAndWaitForFinishOfDisposalAsync(node);

                var rehab = await WaitForValueAsync(() => GetRehabCount(store, name), 1);
                Assert.Equal(1, rehab);

                cluster.Leader.ServerStore.Engine.CurrentLeader.StepDown();

                await Task.Delay(3_000);

                var members = await GetMembersCount(store, name);
                Assert.Equal(1, members);

                rehab = await GetRehabCount(store, name);
                Assert.Equal(1, rehab);

                await Task.Delay(7_000);

                members = await GetMembersCount(store, name);
                Assert.Equal(2, members);
            }
        }

        [Fact]
        public async Task WaitMoveToRehabGraceTime()
        {
            var clusterSize = 3;
            var cluster = await CreateRaftCluster(clusterSize, true, 0, customSettings: new Dictionary<string, string>
            {
                [RavenConfiguration.GetKey(x => x.Cluster.MoveToRehabGraceTime)] = "10",
                [RavenConfiguration.GetKey(x => x.Cluster.StabilizationTime)] = "1",
            });
            using (var store = new DocumentStore
            {
                Urls = new[] { cluster.Leader.WebUrl },
            }.Initialize())
            {
                var name = GetDatabaseName();
                var doc = new DatabaseRecord(name)
                {
                    Topology = new DatabaseTopology
                    {
                        Members = new List<string>
                        {
                            "A",
                            "B"
                        },
                        ReplicationFactor = 2,
                    }
                };
                var databaseResult = await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(doc, 2));
                Assert.Equal(2, databaseResult.Topology.Members.Count);

                var node = cluster.Nodes.Single(n => n.ServerStore.NodeTag == "B");
                await DisposeServerAndWaitForFinishOfDisposalAsync(node);

                cluster.Leader.ServerStore.Engine.CurrentLeader.StepDown();

                await Task.Delay(3_000);

                var members = await GetMembersCount(store, name);
                Assert.Equal(2, members);

                var rehab = await GetRehabCount(store, name);
                Assert.Equal(0, rehab);

                await Task.Delay(10_000);

                members = await GetMembersCount(store, name);
                Assert.Equal(1, members);

                rehab = await GetRehabCount(store, name);
                Assert.Equal(1, rehab);
            }
        }
    }
}
