using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Server;
using Raven.Server.Config;
using Raven.Server.Config.Categories;
using Raven.Server.Rachis;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Tests.Infrastructure;
using Xunit;

namespace RachisTests.DatabaseCluster
{
    public class ClusterDatabaseMaintenance : ClusterTestBase
    {
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
            using (var server = GetNewServer(new Dictionary<string, string>
            {
                [RavenConfiguration.GetKey(x => x.Core.ServerUrls)] = "http://0.0.0.0:0",
                [RavenConfiguration.GetKey(x => x.Security.UnsecuredAccessAllowed)] = UnsecuredAccessAddressRange.PublicNetwork.ToString()
            }))
            using (var store = GetDocumentStore(new Options
            {
                Server = server,
                ModifyDocumentStore = documentStore => documentStore.Urls = new []{server.ServerStore.GetNodeHttpServerUrl()},
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
                Server = leader
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
                DisposeServerAndWaitForFinishOfDisposal(Servers[1]);
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
                    Assert.Equal(2, database.DocumentsStorage.GetLastTombstoneEtag(ctx, "Users"));
                }
            }
        }

        [Fact]
        public async Task MoveToRehabOnServerDown()
        {
            var clusterSize = 3;
            var databaseName = "DemoteOnServerDown";
            var leader = await CreateRaftClusterAndGetLeader(clusterSize, true, 0, customSettings: new Dictionary<string, string>
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
                Servers[1].Dispose();

                var val = await WaitForValueAsync(async () => await GetMembersCount(store, databaseName), clusterSize - 1);
                Assert.Equal(clusterSize - 1, val);
                val = await WaitForValueAsync(async () => await GetRehabCount(store, databaseName), 1);
                Assert.Equal(1, val);
            }
        }

        [Fact]
        public async Task PromoteOnCatchingUp()
        {
            var clusterSize = 3;
            var databaseName = "PromoteOnCatchingUp";
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
            var databaseName = "SuccessfulMaintenanceOnLeaderChange";
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
            var databaseName = "PromoteDatabaseNodeBackAfterReconnection";
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
                var urls = new[] { Servers[1].WebUrl };
                var dataDir = Servers[1].Configuration.Core.DataDirectory.FullPath.Split('/').Last();
                DisposeServerAndWaitForFinishOfDisposal(Servers[1]);

                var val = await WaitForValueAsync(async () => await GetMembersCount(store, databaseName), clusterSize - 1);
                Assert.Equal(clusterSize - 1, val);
                val = await WaitForValueAsync(async () => await GetRehabCount(store, databaseName), 1);
                Assert.Equal(1, val);
                WaitForUserToContinueTheTest(urls[0]);
                Servers[1] = GetNewServer(new Dictionary<string, string> { { RavenConfiguration.GetKey(x => x.Core.ServerUrls), urls[0] } }, runInMemory: false, deletePrevious: false, partialPath: dataDir);
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
            var databaseName = "MoveToPassiveWhenRefusedConnectionFromAllNodes";
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
                var dataDir = Servers[1].Configuration.Core.DataDirectory.FullPath.Split('/').Last();
                var urls = new[] { Servers[1].WebUrl };
                var nodeTag = Servers[1].ServerStore.NodeTag;
                // kill the process and remove the node from topology
                DisposeServerAndWaitForFinishOfDisposal(Servers[1]);

                await ActionWithLeader((l) => l.ServerStore.RemoveFromClusterAsync(nodeTag));

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
                Servers[1] = GetNewServer(new Dictionary<string, string>
                {
                    {RavenConfiguration.GetKey(x => x.Core.PublicServerUrl), urls[0]},
                    {RavenConfiguration.GetKey(x => x.Core.ServerUrls), urls[0]},
                    {RavenConfiguration.GetKey(x => x.Cluster.ElectionTimeout), "600"}

                }, runInMemory: false, deletePrevious: false, partialPath: dataDir);

                Assert.True(await Servers[1].ServerStore.WaitForState(RachisState.Passive, CancellationToken.None).WaitAsync(TimeSpan.FromSeconds(30)), "1st assert");
                // rejoin the node to the cluster

                await ActionWithLeader( (l) => l.ServerStore.AddNodeToClusterAsync(urls[0], nodeTag));

                Assert.True(await Servers[1].ServerStore.WaitForState(RachisState.Follower, CancellationToken.None).WaitAsync(TimeSpan.FromSeconds(30)), "2nd assert");
            }
        }

        [Fact]
        public async Task RedistrebuteDatabaseIfNodeFailes()
        {
            DebuggerAttachedTimeout.DisableLongTimespan = true;
            var clusterSize = 3;
            var dbGroupSize = 2;
            var databaseName = "RedistrebuteDatabaseIfNodeFailes";
            var leader = await CreateRaftClusterAndGetLeader(clusterSize, false, 0);

            using (var store = new DocumentStore
            {
                Urls = new[] { leader.WebUrl },
                Database = databaseName
            }.Initialize())
            {
                var doc = new DatabaseRecord(databaseName);
                doc.Topology = new DatabaseTopology();
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
        public async Task RedistrebuteDatabaseOnMultiFailure()
        {
            DebuggerAttachedTimeout.DisableLongTimespan = true;
            var clusterSize = 5;
            var dbGroupSize = 3;
            var databaseName = "RedistrebuteDatabaseOnCascadeFailure";
            var leader = await CreateRaftClusterAndGetLeader(clusterSize, false, 0);

            using (var store = new DocumentStore
            {
                Urls = new[] { leader.WebUrl },
                Database = databaseName
            }.Initialize())
            {
                var doc = new DatabaseRecord(databaseName);
                doc.Topology = new DatabaseTopology();
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
            var databaseName = "RemoveNodeFromClusterWhileDeletion" + Guid.NewGuid();
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
            var databaseName = "DontRemoveNodeWhileItHasNotReplicatedDocs" + Guid.NewGuid();
            var leader = await CreateRaftClusterAndGetLeader(3, shouldRunInMemory: false);

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
                var urlsA = new[] { serverA.WebUrl };
                var dataDirA = serverA.Configuration.Core.DataDirectory.FullPath.Split('/').Last();
                DisposeServerAndWaitForFinishOfDisposal(serverA);

                var serverB = Servers.Single(s => s.ServerStore.NodeTag == "B");
                var urlsB = new[] { serverB.WebUrl };
                var dataDirB = serverB.Configuration.Core.DataDirectory.FullPath.Split('/').Last();
                DisposeServerAndWaitForFinishOfDisposal(serverB);

                // write doc only to C
                using (var session = leaderStore.OpenSession())
                {
                    session.Store(new User(), "users/2");
                    session.SaveChanges();
                }

                var serverC = Servers.Single(s => s.ServerStore.NodeTag == "C");
                var urlsC = new[] { serverC.WebUrl };
                var dataDirC = serverC.Configuration.Core.DataDirectory.FullPath.Split('/').Last();
                DisposeServerAndWaitForFinishOfDisposal(serverC);

                Servers[0] = GetNewServer(new Dictionary<string, string> { { RavenConfiguration.GetKey(x => x.Core.ServerUrls), urlsA[0] } }, runInMemory: false, deletePrevious: false, partialPath: dataDirA);
                Servers[1] = GetNewServer(new Dictionary<string, string> { { RavenConfiguration.GetKey(x => x.Core.ServerUrls), urlsB[0] } }, runInMemory: false, deletePrevious: false, partialPath: dataDirB);
                await Task.Delay(TimeSpan.FromSeconds(10));
                Assert.Equal(2, await WaitForValueAsync(async () => await GetMembersCount(leaderStore, databaseName), 2));
                Assert.Equal(1, await WaitForValueAsync(async () => await GetRehabCount(leaderStore, databaseName), 1));

                using (var session = leaderStore.OpenSession())
                {
                    session.Store(new User(), "users/3");
                    session.SaveChanges();
                }
                Assert.True(await WaitForDocumentInClusterAsync<User>(new DatabaseTopology
                {
                    Members = new List<string> { "A", "B" }
                }, databaseName, "users/3", null, TimeSpan.FromSeconds(10)));

                Servers[2] = GetNewServer(new Dictionary<string, string> { { RavenConfiguration.GetKey(x => x.Core.ServerUrls), urlsC[0] } }, runInMemory: false, deletePrevious: false, partialPath: dataDirC);
                Assert.Equal(2, await WaitForValueAsync(async () => await GetMembersCount(leaderStore, databaseName), 2));
                Assert.Equal(0, await WaitForValueAsync(async () => await GetRehabCount(leaderStore, databaseName), 0, 30_000));
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
            var databaseName = "Promote_immedtialty_should_work" + Guid.NewGuid();
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
            var databaseName = "ChangeUrlOfSingleNodeCluster" + Guid.NewGuid();
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
            var dataDir = Servers[0].Configuration.Core.DataDirectory.FullPath.Split('/').Last();
            DisposeServerAndWaitForFinishOfDisposal(Servers[0]);
            var customSettings = new Dictionary<string, string>();
            var certPath = SetupServerAuthentication(customSettings);
            customSettings[RavenConfiguration.GetKey(x => x.Core.ServerUrls)] = "https://" + Environment.MachineName + ":8999";
            leader = Servers[0] = GetNewServer(customSettings, runInMemory: false, deletePrevious: false, partialPath: dataDir);

            var adminCert = AskServerForClientCertificate(certPath, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin, server: leader);

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
            var newUrl = "http://" + Environment.MachineName + ":0";
            string nodeTag;

            var leader = await CreateRaftClusterAndGetLeader(groupSize, shouldRunInMemory: false, leaderIndex: 0, customSettings: new Dictionary<string, string>
            {
                [RavenConfiguration.GetKey(x => x.Cluster.MoveToRehabGraceTime)] = "4"
            });

            using (var leaderStore = new DocumentStore
            {
                Urls = new[] {leader.WebUrl},
                Database = databaseName
            })
            {
                leaderStore.Initialize();
                await CreateDatabaseInCluster(databaseName, groupSize, leader.WebUrl);

                var dbToplogy = (await leaderStore.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(databaseName))).Topology;
                Assert.Equal(groupSize, dbToplogy.Members.Count);

                var dataDir = Servers[1].Configuration.Core.DataDirectory.FullPath.Split('/').Last();
                nodeTag = Servers[1].ServerStore.NodeTag;
                // kill and change the url
                DisposeServerAndWaitForFinishOfDisposal(Servers[1]);
                var customSettings = new Dictionary<string, string>
                {
                    [RavenConfiguration.GetKey(x => x.Core.ServerUrls)] = newUrl,
                    [RavenConfiguration.GetKey(x => x.Security.UnsecuredAccessAllowed)] = UnsecuredAccessAddressRange.PublicNetwork.ToString()
                };
                Servers[1] = GetNewServer(customSettings, runInMemory: false, deletePrevious: false, partialPath: dataDir);
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

        private static async Task<int> GetPromotableCount(IDocumentStore store, string databaseName)
        {
            var res = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(databaseName));
            if (res == null)
            {
                return -1;
            }
            return res.Topology.Promotables.Count;
        }

        private static async Task<int> GetRehabCount(IDocumentStore store, string databaseName)
        {
            var res = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(databaseName));
            if (res == null)
            {
                return -1;
            }
            return res.Topology.Rehabs.Count;
        }

        private static async Task<int> GetMembersCount(IDocumentStore store, string databaseName)
        {
            var res = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(databaseName));
            if (res == null)
            {
                return -1;
            }
            return res.Topology.Members.Count;
        }
    }
}
