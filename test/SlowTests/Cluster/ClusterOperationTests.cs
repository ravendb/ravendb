using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Conventions;
using Raven.Client.Exceptions;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Commands;
using Raven.Client.ServerWide.Operations;
using Raven.Server;
using Raven.Server.Config;
using Tests.Infrastructure;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Json;
using Xunit;

namespace SlowTests.Cluster
{
    public class ClusterOperationTests : ClusterTestBase
    {
        [Fact]
        public async Task ReorderDatabaseNodes()
        {
            var db = "ReorderDatabaseNodes";
            var leader = await CreateRaftClusterAndGetLeader(3);
            await CreateDatabaseInCluster(db, 3, leader.WebUrl);
            using (var store = new DocumentStore
            {
                Database = db,
                Urls = new[] { leader.WebUrl }
            }.Initialize())
            {
                await ReverseOrderSuccessfully(store, db);
                await FailSuccessfully(store, db);
            }
        }

        public static async Task FailSuccessfully(IDocumentStore store, string db)
        {
            var ex = await Assert.ThrowsAsync<RavenException>(async () =>
            {
                await store.Maintenance.Server.SendAsync(new ReorderDatabaseMembersOperation(db, new List<string>()
                {
                    "A",
                    "B"
                }));
            });
            Assert.True(ex.InnerException is ArgumentException);
            ex = await Assert.ThrowsAsync<RavenException>(async () =>
            {
                await store.Maintenance.Server.SendAsync(new ReorderDatabaseMembersOperation(db, new List<string>()
                {
                    "C",
                    "B",
                    "A",
                    "F"
                }));
            });
            Assert.True(ex.InnerException is ArgumentException);
        }

        [Fact]
        public async Task ClusterWideIdentity()
        {
            var db = "ClusterWideIdentity";
            var leader = await CreateRaftClusterAndGetLeader(2);
            await CreateDatabaseInCluster(db, 2, leader.WebUrl);
            var nonLeader = Servers.First(x => ReferenceEquals(x, leader) == false);
            using (var store = new DocumentStore
            {
                Database = db,
                Urls = new[] { nonLeader.WebUrl }
            }.Initialize())
            {
                using (var session = store.OpenAsyncSession())
                {
                    var command = new SeedIdentityForCommand("users", 1990);

                    await session.Advanced.RequestExecutor.ExecuteAsync(command, session.Advanced.Context);

                    var result = command.Result;

                    Assert.Equal(1990, result);
                    var user = new User
                    {
                        Name = "Adi",
                        LastName = "Async"
                    };
                    await session.StoreAsync(user, "users|");
                    await session.SaveChangesAsync();
                    var id = session.Advanced.GetDocumentId(user);
                    Assert.Equal("users/1991", id);
                }
            }
        }

        [Fact]
        public async Task ChangesApiFailOver()
        {
            var db = "Test";
            var topology = new DatabaseTopology
            {
                DynamicNodesDistribution = true
            };
            var leader = await CreateRaftClusterAndGetLeader(3, customSettings: new Dictionary<string, string>()
            {
                [RavenConfiguration.GetKey(x => x.Cluster.AddReplicaTimeout)] = "1",
                [RavenConfiguration.GetKey(x => x.Cluster.MoveToRehabGraceTime)] = "0",
                [RavenConfiguration.GetKey(x => x.Cluster.StabilizationTime)] = "1",
                [RavenConfiguration.GetKey(x => x.Cluster.ElectionTimeout)] = "50"
            });

            await CreateDatabaseInCluster(new DatabaseRecord
            {
                DatabaseName = db,
                Topology = topology
            }, 2, leader.WebUrl);

            using (var store = new DocumentStore
            {
                Database = db,
                Urls = new[] { leader.WebUrl }
            }.Initialize())
            {
                var list = new BlockingCollection<DocumentChange>();
                var taskObservable = store.Changes();
                await taskObservable.EnsureConnectedNow();
                var observableWithTask = taskObservable.ForDocument("users/1");
                observableWithTask.Subscribe(list.Add);
                await observableWithTask.EnsureSubscribedNow();

                using (var session = store.OpenSession())
                {
                    session.Store(new User(), "users/1");
                    session.SaveChanges();
                }

                WaitForDocument(store, "users/1");

                var value = WaitForValue(() => list.Count, 1);
                Assert.Equal(1, value);

                var currentUrl = store.GetRequestExecutor().Url;
                RavenServer toDispose = null;
                RavenServer workingServer = null;

                DisposeCurrentServer(currentUrl, ref toDispose, ref workingServer);

                await taskObservable.EnsureConnectedNow();

                WaitForTopologyStabilization(db, workingServer, 1, 2);

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User(), "users/1");
                    await session.SaveChangesAsync();
                }
                value = WaitForValue(() => list.Count, 2);
                Assert.Equal(2, value);

                currentUrl = store.GetRequestExecutor().Url;
                DisposeCurrentServer(currentUrl, ref toDispose, ref workingServer);

                await taskObservable.EnsureConnectedNow();

                WaitForTopologyStabilization(db, workingServer, 2, 1);

                using (var session = store.OpenSession())
                {
                    session.Store(new User(), "users/1");
                    session.SaveChanges();
                }
                value = WaitForValue(() => list.Count, 3);
                Assert.Equal(3, value);
            }
        }

        [Fact]
        public async Task ChangesApiReorderDatabaseNodes()
        {
            var db = "ReorderDatabaseNodes";
            var leader = await CreateRaftClusterAndGetLeader(2);
            await CreateDatabaseInCluster(db, 2, leader.WebUrl);
            using (var store = new DocumentStore
            {
                Database = db,
                Urls = new[] { leader.WebUrl }
            }.Initialize())
            {
                var list = new BlockingCollection<DocumentChange>();
                var taskObservable = store.Changes();
                await taskObservable.EnsureConnectedNow();
                var observableWithTask = taskObservable.ForDocument("users/1");
                observableWithTask.Subscribe(list.Add);
                await observableWithTask.EnsureSubscribedNow();

                using (var session = store.OpenSession())
                {
                    session.Store(new User(), "users/1");
                    session.SaveChanges();
                }
                string url1 = store.GetRequestExecutor().Url;
                Assert.True(WaitForDocument(store, "users/1"));
                var value = WaitForValue(() => list.Count, 1);
                Assert.Equal(1, value);


                await ReverseOrderSuccessfully(store, db);

                using (var session = store.OpenSession())
                {
                     session.Store(new User(), "users/1");
                     session.SaveChanges();
                }
                value = WaitForValue(() => list.Count, 2);
                Assert.Equal(2, value);
                string url2 = store.GetRequestExecutor().Url;
                Assert.NotEqual(url1, url2);
            }
        }

        private void DisposeCurrentServer(string currnetUrl, ref RavenServer toDispose, ref RavenServer workingServer)
        {
            foreach (var server in Servers)
            {
                if (server.WebUrl == currnetUrl)
                {
                    toDispose = server;
                    continue;
                }
                if (server.Disposed != true)
                    workingServer = server;
            }
            DisposeServerAndWaitForFinishOfDisposal(toDispose);
        }

        public void WaitForTopologyStabilization(string s, RavenServer workingServer, int rehabCount, int memberCount)
        {
            using (var tempStore = new DocumentStore
            {
                Database = s,
                Urls = new[] { workingServer.WebUrl },
                Conventions = new DocumentConventions
                    { DisableTopologyUpdates = true }
            }.Initialize())
            {
                Topology topo;
                using (var context = JsonOperationContext.ShortTermSingleUse())
                {
                    var value = WaitForValue(() =>
                    {
                        var topologyGetCommand = new GetDatabaseTopologyCommand();
                        tempStore.GetRequestExecutor().Execute(topologyGetCommand, context);
                        topo = topologyGetCommand.Result;
                        int rehab = 0;
                        int members = 0;
                        topo.Nodes.ForEach(n =>
                        {
                            switch (n.ServerRole)
                            {
                                case ServerNode.Role.Rehab:
                                    rehab++;
                                    break;
                                case ServerNode.Role.Member:
                                    members++;
                                    break;
                            }
                        });
                        return new Tuple<int, int>(rehab, members);

                    }, new Tuple<int, int>(rehabCount, memberCount));
                }
            }
        }

        public static async Task ReverseOrderSuccessfully(IDocumentStore store, string db)
        {
            var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(db));
            record.Topology.Members.Reverse();
            var copy = new List<string>(record.Topology.Members);
            await store.Maintenance.Server.SendAsync(new ReorderDatabaseMembersOperation(db, record.Topology.Members));
            record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(db));
            Assert.True(copy.All(record.Topology.Members.Contains));
        }
    }
}
