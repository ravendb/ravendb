using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests.Server.Documents.Indexing;
using FastTests.Server.Replication;
using Raven.Client.Documents;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Server.Config;
using Raven.Server.Config.Categories;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;

namespace RachisTests.DatabaseCluster
{
    public class ClusterDatabaseMaintenance : ReplicationTestBase
    {
        [NightlyBuildFact]
        public async Task MoveToRehabOnServerDown()
        {
            var clusterSize = 3;
            var databaseName = "DemoteOnServerDown";
            var leader = await CreateRaftClusterAndGetLeader(clusterSize, true, 0);
            using (var store = new DocumentStore
            {
                Urls = new[] {leader.WebUrl},
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

        [NightlyBuildFact]
        public async Task PromoteOnCatchingUp()
        {
            var clusterSize = 3;
            var databaseName = "PromoteOnCatchingUp";
            var leader = await CreateRaftClusterAndGetLeader(clusterSize, true, 0);
            using (var store = new DocumentStore
            {
                Urls = new[] {leader.WebUrl},
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
                    Urls = new[] {dbServer.WebUrl},
                    Database = databaseName
                }.Initialize())
                {
                    using (var session = dbStore.OpenAsyncSession())
                    {
                        await session.StoreAsync(new IndexMerging.User { Name = "Karmel" }, "users/1");
                        await session.SaveChangesAsync();
                    }
                }

                var res = await store.Maintenance.Server.SendAsync(new AddDatabaseNodeOperation(databaseName));
                Assert.Equal(1, res.Topology.Members.Count);
                Assert.Equal(1, res.Topology.Promotables.Count);

                await WaitForRaftIndexToBeAppliedInCluster(res.RaftCommandIndex, TimeSpan.FromSeconds(5));
                await WaitForDocumentInClusterAsync<IndexMerging.User>(res.Topology, databaseName, "users/1", u => u.Name == "Karmel", TimeSpan.FromSeconds(10));
                await Task.Delay(TimeSpan.FromSeconds(5)); // wait for the observer 
                var val = await WaitForValueAsync(async () => await GetPromotableCount(store, databaseName), 0);
                Assert.Equal(0, val);
                val = await WaitForValueAsync(async () => await GetMembersCount(store, databaseName), 2);
                Assert.Equal(2, val);
            }
        }

        [NightlyBuildFact]
        public async Task SuccessfulMaintenanceOnLeaderChange()
        {
            var clusterSize = 3;
            var databaseName = "SuccessfulMaintenanceOnLeaderChange";
            var leader = await CreateRaftClusterAndGetLeader(clusterSize, true, 0);
            using (var store = new DocumentStore()
            {
                Urls = new[] {leader.WebUrl},
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
                Urls = new[] {Servers[1].WebUrl},
                Database = databaseName
            }.Initialize())
            {
                var val = await WaitForValueAsync(async () => await GetMembersCount(store, databaseName), 2);
                Assert.Equal(2, val);
                val = await WaitForValueAsync(async () => await GetRehabCount(store, databaseName), 1);
                Assert.Equal(1, val);
            }
        }

        [NightlyBuildFact]
        public async Task PromoteDatabaseNodeBackAfterReconnection()
        {
            var clusterSize = 3;
            var databaseName = "PromoteDatabaseNodeBackAfterReconnection";
            var leader = await CreateRaftClusterAndGetLeader(clusterSize, false, 0);
            using (var store = new DocumentStore
            {
                Urls = new[] {leader.WebUrl},
                Database = databaseName
            }.Initialize())
            {
                var doc = new DatabaseRecord(databaseName);
                var databaseResult = await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(doc, clusterSize));
                Assert.Equal(clusterSize, databaseResult.Topology.Members.Count);
                await WaitForRaftIndexToBeAppliedInCluster(databaseResult.RaftCommandIndex, TimeSpan.FromSeconds(10));
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new IndexMerging.User());
                    await session.SaveChangesAsync();
                }
                var urls = new[] {Servers[1].WebUrl};
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

        [NightlyBuildFact]
        public async Task MoveToPassiveWhenRefusedConnectionFromAllNodes()
        {
            //DebuggerAttachedTimeout.DisableLongTimespan = true;
            var clusterSize = 3;
            var databaseName = "MoveToPassiveWhenRefusedConnectionFromAllNodes";
            var leader = await CreateRaftClusterAndGetLeader(clusterSize, false, 0);

            using (var store = new DocumentStore
            {
                Urls = new[] {leader.WebUrl},
                Database = databaseName
            }.Initialize())
            {
                var doc = new DatabaseRecord(databaseName);
                var databaseResult = await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(doc, clusterSize));
                Assert.Equal(clusterSize, databaseResult.Topology.Members.Count);
                await WaitForRaftIndexToBeAppliedInCluster(databaseResult.RaftCommandIndex, TimeSpan.FromSeconds(10));
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new IndexMerging.User());
                    await session.SaveChangesAsync();
                }
                var dataDir = Servers[1].Configuration.Core.DataDirectory.FullPath.Split('/').Last();
                var urls = new[] {Servers[1].WebUrl};
                var nodeTag = Servers[1].ServerStore.NodeTag;
                // kill the process and remove the node from topology
                DisposeServerAndWaitForFinishOfDisposal(Servers[1]);
                await leader.ServerStore.RemoveFromClusterAsync(nodeTag);
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
                Servers[1] = GetNewServer(new Dictionary<string, string> { { RavenConfiguration.GetKey(x => x.Core.PublicServerUrl), urls[0] }, { RavenConfiguration.GetKey(x => x.Core.ServerUrls), urls[0] } }, runInMemory: false, deletePrevious: false, partialPath: dataDir);
                await Servers[1].ServerStore.WaitForState(RachisState.Passive).WaitAsync(TimeSpan.FromSeconds(30));
                Assert.Equal(RachisState.Passive, Servers[1].ServerStore.CurrentRachisState);
                // rejoin the node to the cluster
                await leader.ServerStore.AddNodeToClusterAsync(urls[0], nodeTag);
                await Servers[1].ServerStore.WaitForState(RachisState.Follower).WaitAsync(TimeSpan.FromSeconds(300));
                Assert.Equal(RachisState.Follower, Servers[1].ServerStore.CurrentRachisState);
            }
        }

        [NightlyBuildFact]
        public async Task RedistrebuteDatabaseIfNodeFailes()
        {
            DebuggerAttachedTimeout.DisableLongTimespan = true;
            var clusterSize = 3;
            var dbGroupSize = 2;
            var databaseName = "RedistrebuteDatabaseIfNodeFailes";
            var leader = await CreateRaftClusterAndGetLeader(clusterSize, false, 0);

            using (var store = new DocumentStore
            {
                Urls = new[] {leader.WebUrl},
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
                    await session.StoreAsync(new IndexMerging.User { Name = "Karmel" }, "users/1");
                    await session.SaveChangesAsync();
                }
                Assert.True(await WaitForDocumentInClusterAsync<IndexMerging.User>(doc.Topology, databaseName, "users/1", u => u.Name == "Karmel", TimeSpan.FromSeconds(5)));
                DisposeServerAndWaitForFinishOfDisposal(Servers[1]);

                // the db should move from node B to node C
                var newTopology = new DatabaseTopology();
                newTopology.Members.Add("A");
                newTopology.Members.Add("C");
                Assert.True(await WaitForDocumentInClusterAsync<IndexMerging.User>(newTopology, databaseName, "users/1", u => u.Name == "Karmel", TimeSpan.FromSeconds(60)));
                var members = await WaitForValueAsync(async () => await GetMembersCount(store, databaseName), 2, 30_000);
                Assert.Equal(2, members);
            }
        }

        [NightlyBuildFact]
        public async Task RedistrebuteDatabaseOnMultiFailure()
        {
            DebuggerAttachedTimeout.DisableLongTimespan = true;
            var clusterSize = 5;
            var dbGroupSize = 3;
            var databaseName = "RedistrebuteDatabaseOnCascadeFailure";
            var leader = await CreateRaftClusterAndGetLeader(clusterSize, false, 0);

            using (var store = new DocumentStore
            {
                Urls = new[] {leader.WebUrl},
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
                    await session.StoreAsync(new IndexMerging.User { Name = "Karmel" }, "users/1");
                    await session.SaveChangesAsync();
                }
                Assert.True(await WaitForDocumentInClusterAsync<IndexMerging.User>(doc.Topology, databaseName, "users/1", u => u.Name == "Karmel", TimeSpan.FromSeconds(5)));
                DisposeServerAndWaitForFinishOfDisposal(Servers[1]);
                WaitForUserToContinueTheTest((DocumentStore)store);
                DisposeServerAndWaitForFinishOfDisposal(Servers[2]);

                // the db should move to D & E
                var newTopology = new DatabaseTopology();
                newTopology.Members.Add("A");
                newTopology.Members.Add("D");
                newTopology.Members.Add("E");
                Assert.True(await WaitForDocumentInClusterAsync<IndexMerging.User>(newTopology, databaseName, "users/1", u => u.Name == "Karmel", TimeSpan.FromSeconds(60)));
                var members = await WaitForValueAsync(async () => await GetMembersCount(store, databaseName), 3, 30_000);
                Assert.Equal(3, members);
            }
        }

        [NightlyBuildFact]
        public async Task RemoveNodeFromClusterWhileDeletion()
        {
            var databaseName = "RemoveNodeFromClusterWhileDeletion" + Guid.NewGuid();
            var leader = await CreateRaftClusterAndGetLeader(3);

            using (var leaderStore = new DocumentStore
            {
                Urls = new[] {leader.WebUrl},
                Database = databaseName,
            })
            {
                leaderStore.Initialize();

                var (index, dbGroupNodes) = await CreateDatabaseInCluster(databaseName, 3, leader.WebUrl);
                await WaitForRaftIndexToBeAppliedInCluster(index, TimeSpan.FromSeconds(30));
                var dbToplogy = (await leaderStore.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(databaseName))).Topology;

                Assert.Equal(3, dbToplogy.AllNodes.Count());
                Assert.Equal(0, dbToplogy.Promotables.Count);

                var node = Servers[1].ServerStore.Engine.Tag;
                DisposeServerAndWaitForFinishOfDisposal(Servers[1]);
                await leaderStore.Maintenance.Server.SendAsync(new DeleteDatabasesOperation(databaseName, true));

                await WaitForValueAsync(async () =>
                {
                    var records = await leaderStore.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(databaseName));
                    return records.DeletionInProgress.Count;
                }, 1);

                DatabaseRecord record = await leaderStore.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(databaseName));
                Assert.Single(record.DeletionInProgress);
                Assert.Equal(node, record.DeletionInProgress.First().Key);
                await leader.ServerStore.RemoveFromClusterAsync(node);

                record = await leaderStore.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(databaseName));

                Assert.Null(record);
            }
        }

        [NightlyBuildFact]
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
                    session.Store(new User(),"users/1");
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
                    Members = new List<string>{"A","B"}
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

        [NightlyBuildFact]
        public async Task Promote_immedtialty_should_work()
        {
            var databaseName = "Promote_immedtialty_should_work" + Guid.NewGuid();
            var leader = await CreateRaftClusterAndGetLeader(3);

            using (var leaderStore = new DocumentStore
            {
                Urls = new[] {leader.WebUrl},
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

        [NightlyBuildFact]
        public async Task ChangeUrlOfSingleNodeCluster()
        {
            var databaseName = "ChangeUrlOfSingleNodeCluster" + Guid.NewGuid();
            var leader = await CreateRaftClusterAndGetLeader(1, shouldRunInMemory:false);

            using (var leaderStore = new DocumentStore
            {
                Urls = new[] {leader.WebUrl},
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
                Urls = new[] {leader.WebUrl},
                Database = databaseName,
            })
            {
                leaderStore.Initialize();
                await Task.Delay(TimeSpan.FromSeconds(5)); // wait for the observer to update the status
                var dbToplogy = (await leaderStore.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(databaseName))).Topology;
                Assert.Equal(1, dbToplogy.Members.Count);
            }
        }

        [NightlyBuildFact]
        public async Task ChangeUrlOfMultiNodeCluster()
        {
            var databaseName = "ChangeUrlOfSingleNodeCluster" + Guid.NewGuid();
            var leader = await CreateRaftClusterAndGetLeader(3, shouldRunInMemory: false);
            var groupSize = 3;
            using (var leaderStore = new DocumentStore
            {
                Urls = new[] {leader.WebUrl},
                Database = databaseName,
            })
            {
                leaderStore.Initialize();
                var (index, dbGroupNodes) = await CreateDatabaseInCluster(databaseName, groupSize, leader.WebUrl);
                await Task.Delay(TimeSpan.FromSeconds(10));
                var dbToplogy = (await leaderStore.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(databaseName))).Topology;
                Assert.Equal(groupSize, dbToplogy.Members.Count);
           
                var dataDir = Servers[1].Configuration.Core.DataDirectory.FullPath.Split('/').Last();
                var nodeTag = Servers[1].ServerStore.NodeTag;
                // kill and change the url
                DisposeServerAndWaitForFinishOfDisposal(Servers[1]);
                var customSettings = new Dictionary<string, string>
                {
                    [RavenConfiguration.GetKey(x => x.Core.ServerUrls)] = "http://" + Environment.MachineName + ":8080",
                    [RavenConfiguration.GetKey(x => x.Security.UnsecuredAccessAllowed)] = UnsecuredAccessAddressRange.PrivateNetwork.ToString()
                };
                Servers[1] = GetNewServer(customSettings, runInMemory: false, deletePrevious: false, partialPath: dataDir);
                // ensure that at this point we still can't talk to node 
                await Task.Delay(TimeSpan.FromSeconds(5)); // wait for the observer to update the status
                dbToplogy = (await leaderStore.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(databaseName))).Topology;
                Assert.Equal(1, dbToplogy.Rehabs.Count);
                Assert.Equal(groupSize - 1, dbToplogy.Members.Count);

                await WaitForLeader(TimeSpan.FromSeconds(5));
                leader = Servers.Single(s => s.Disposed == false && s.ServerStore.IsLeader());
                // remove and rejoin to change the url
                await leader.ServerStore.RemoveFromClusterAsync(nodeTag);
                await leader.ServerStore.AddNodeToClusterAsync(Servers[1].ServerStore.GetNodeHttpServerUrl(), nodeTag);
                await Servers[1].ServerStore.WaitForState(RachisState.Follower);
                await Task.Delay(TimeSpan.FromSeconds(5)); // wait for the observer to update the status
                dbToplogy = (await leaderStore.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(databaseName))).Topology;
                Assert.Equal(groupSize, dbToplogy.Members.Count);
            }
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
