using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net.Http;
using System.Threading;
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
using Raven.Client.Documents.Operations.Identities;
using Raven.Client.Extensions;
using Raven.Server.Utils;
using Sparrow.Server;
using Xunit.Abstractions;

namespace SlowTests.Cluster
{
    public class ClusterOperationTests : ClusterTestBase
    {
        public ClusterOperationTests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task ReorderDatabaseNodes()
        {
            var db = "ReorderDatabaseNodes";
            var (_, leader) = await CreateRaftCluster(3);
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
            var (_, leader) = await CreateRaftCluster(2);
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
                    var result = store.Maintenance.SendAsync(new SeedIdentityForOperation("users", 1990));
                    Assert.Equal(1990, result.Result);

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
        public async Task NextIdentityForOperationShouldBroadcast()
        {
            DebuggerAttachedTimeout.DisableLongTimespan = true;
            DefaultClusterSettings[RavenConfiguration.GetKey(x => x.Cluster.RotatePreferredNodeGraceTime)] = "15";
            DefaultClusterSettings[RavenConfiguration.GetKey(x => x.Cluster.MoveToRehabGraceTime)] = "15";

            var database = GetDatabaseName();
            var numberOfNodes = 3;
            var cluster = await CreateRaftCluster(numberOfNodes);
            var createResult = await CreateDatabaseInClusterInner(new DatabaseRecord(database), numberOfNodes, cluster.Leader.WebUrl, null);

            using (var store = new DocumentStore
            {
                Database = database,
                Urls = new[] { cluster.Leader.WebUrl }
            }.Initialize())
            {

                var re = store.GetRequestExecutor(database);
                var result = store.Maintenance.ForDatabase(database).Send(new NextIdentityForOperation("person|"));
                Assert.Equal(1, result);

                var preferred = await re.GetPreferredNode();
                var tag = preferred.Item2.ClusterTag;
                var server = createResult.Servers.Single(s => s.ServerStore.NodeTag == tag);
                server.ServerStore.InitializationCompleted.Reset(true);
                server.ServerStore.Initialized = false;
                server.ServerStore.Engine.CurrentLeader?.StepDown();

                var sp = Stopwatch.StartNew();
                result = store.Maintenance.ForDatabase(database).Send(new NextIdentityForOperation("person|"));
                Assert.True(sp.Elapsed < TimeSpan.FromSeconds(10));
                var newPreferred = await re.GetPreferredNode();

                Assert.NotEqual(tag, newPreferred.Item2.ClusterTag);
                Assert.Equal(2, result);
            }
        }

        [Fact]
        public async Task NextIdentityForOperationShouldBroadcastAndFail()
        {
            DebuggerAttachedTimeout.DisableLongTimespan = true;

            var database = GetDatabaseName();
            var numberOfNodes = 3;
            var cluster = await CreateRaftCluster(numberOfNodes);
            var createResult = await CreateDatabaseInClusterInner(new DatabaseRecord(database), numberOfNodes, cluster.Leader.WebUrl, null);

            using (var store = new DocumentStore
            {
                Database = database,
                Urls = new[] { cluster.Leader.WebUrl }
            }.Initialize())
            {
                var result = store.Maintenance.ForDatabase(database).Send(new NextIdentityForOperation("person|"));
                Assert.Equal(1, result);

                var node = createResult.Servers.First(n => n != cluster.Leader);
                node.ServerStore.InitializationCompleted.Reset(true);
                node.ServerStore.Initialized = false;

                await ActionWithLeader((l) => DisposeServerAndWaitForFinishOfDisposalAsync(l));

                using (var cancel = new CancellationTokenSource(TimeSpan.FromSeconds(15)))
                {
                    await Task.WhenAll(createResult.Servers.Where(s => s.Disposed == false).Select(s => s.ServerStore.WaitForState(RachisState.Candidate, cancel.Token)));
                }

                var sp = Stopwatch.StartNew();
                var ex = Assert.Throws<AllTopologyNodesDownException>(() => result = store.Maintenance.ForDatabase(database).Send(new NextIdentityForOperation("person|")));
                Assert.True(sp.Elapsed < TimeSpan.FromSeconds(45));

                var ae = (AggregateException)ex.InnerException;
                Assert.NotNull(ae);

                var exceptionTypes = new List<Type>{
                    typeof(HttpRequestException),  // the disposed node
                    typeof(TimeoutException), // the hang node
                    typeof(RavenException) // the last active one (no leader)
                };

                Assert.Contains(ae.InnerExceptions[0].InnerException.GetType(), exceptionTypes);
                Assert.Contains(ae.InnerExceptions[1].InnerException.GetType(), exceptionTypes);
                Assert.Contains(ae.InnerExceptions[2].InnerException.GetType(), exceptionTypes);
            }
        }

        [Fact]
        public async Task PreferredNodeShouldBeRestoredAfterBroadcast()
        {
            DebuggerAttachedTimeout.DisableLongTimespan = true;
            DefaultClusterSettings[RavenConfiguration.GetKey(x => x.Cluster.RotatePreferredNodeGraceTime)] = "15";
            DefaultClusterSettings[RavenConfiguration.GetKey(x => x.Cluster.MoveToRehabGraceTime)] = "15";

            var database = GetDatabaseName();
            var numberOfNodes = 3;
            var cluster = await CreateRaftCluster(numberOfNodes);
            var createResult = await CreateDatabaseInClusterInner(new DatabaseRecord(database), numberOfNodes, cluster.Leader.WebUrl, null);

            using (var store = new DocumentStore
            {
                Database = database,
                Urls = new[] { cluster.Leader.WebUrl }
            }.Initialize())
            {
                var re = store.GetRequestExecutor(database);
                var preferred = await re.GetPreferredNode();
                var tag = preferred.Item2.ClusterTag;

                var result = store.Maintenance.ForDatabase(database).Send(new NextIdentityForOperation("person|"));
                Assert.Equal(1, result);

                preferred = await re.GetPreferredNode();
                Assert.Equal(tag, preferred.Item2.ClusterTag);

                var server = createResult.Servers.Single(s => s.ServerStore.NodeTag == tag);
                server.ServerStore.InitializationCompleted.Reset(true);
                server.ServerStore.Initialized = false;
                server.ServerStore.Engine.CurrentLeader?.StepDown();
                var sp = Stopwatch.StartNew();
                result = store.Maintenance.ForDatabase(database).Send(new NextIdentityForOperation("person|"));
                sp.Stop();
                Assert.True(sp.Elapsed < TimeSpan.FromSeconds(10));

                var newPreferred = await re.GetPreferredNode();
                Assert.NotEqual(tag, newPreferred.Item2.ClusterTag);
                Assert.Equal(2, result);

                server.ServerStore.Initialized = true;

                var current = WaitForValue(() =>
                {
                    var p = re.GetPreferredNode().Result;

                    return p.Item2.ClusterTag;
                }, tag);

                Assert.Equal(tag, current);
            }
        }

        [Fact]
        public async Task ChangesApiFailOver()
        {
            using (var cts = new CancellationTokenSource(TimeSpan.FromMinutes(3)))
            {
                var db = "ChangesApiFailOver_Test";
                var topology = new DatabaseTopology { DynamicNodesDistribution = true };
                var (_, leader) = await CreateRaftCluster(3,
                    customSettings: new Dictionary<string, string>()
                    {
                        [RavenConfiguration.GetKey(x => x.Cluster.AddReplicaTimeout)] = "1",
                        [RavenConfiguration.GetKey(x => x.Cluster.MoveToRehabGraceTime)] = "0",
                        [RavenConfiguration.GetKey(x => x.Cluster.StabilizationTime)] = "1",
                        [RavenConfiguration.GetKey(x => x.Cluster.ElectionTimeout)] = "50"
                    });

                await CreateDatabaseInCluster(new DatabaseRecord { DatabaseName = db, Topology = topology }, 2, leader.WebUrl);

                using (var store = new DocumentStore { Database = db, Urls = new[] { leader.WebUrl } }.Initialize())
                {
                    var list = new BlockingCollection<DocumentChange>();
                    var taskObservable = store.Changes();
                    await taskObservable.EnsureConnectedNow().WithCancellation(cts.Token);
                    var observableWithTask = taskObservable.ForDocument("users/1");
                    observableWithTask.Subscribe(list.Add);
                    await observableWithTask.EnsureSubscribedNow().WithCancellation(cts.Token);

                    using (var session = store.OpenAsyncSession())
                    {
                        await session.StoreAsync(new User(), "users/1", cts.Token);
                        await session.SaveChangesAsync(cts.Token);
                    }

                    WaitForDocument(store, "users/1");

                    var value = await WaitForValueAsync(() => list.Count, 1);
                    Assert.Equal(1, value);

                    var currentUrl = store.GetRequestExecutor().Url;
                    RavenServer toDispose = null;
                    RavenServer workingServer = null;

                    DisposeCurrentServer(currentUrl, ref toDispose, ref workingServer);

                    await taskObservable.EnsureConnectedNow().WithCancellation(cts.Token);

                    await WaitForTopologyStabilizationAsync(db, workingServer, 1, 2).WithCancellation(cts.Token);

                    using (var session = store.OpenAsyncSession())
                    {
                        await session.StoreAsync(new User(), "users/1", cts.Token);
                        await session.SaveChangesAsync(cts.Token);
                    }

                    value = await WaitForValueAsync(() => list.Count, 2);
                    Assert.Equal(2, value);

                    currentUrl = store.GetRequestExecutor().Url;
                    DisposeCurrentServer(currentUrl, ref toDispose, ref workingServer);

                    await taskObservable.EnsureConnectedNow().WithCancellation(cts.Token);

                    await WaitForTopologyStabilizationAsync(db, workingServer, 2, 1).WithCancellation(cts.Token);

                    using (var session = store.OpenSession())
                    {
                        session.Store(new User(), "users/1");
                        session.SaveChanges();
                    }

                    value = await WaitForValueAsync(() => list.Count, 3);
                    Assert.Equal(3, value);
                }
            }
        }

        [Fact]
        public async Task ChangesApiReorderDatabaseNodes()
        {
            var db = "ReorderDatabaseNodes";
            var (_, leader) = await CreateRaftCluster(2);
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

                var value2 = WaitForValue(() =>
                {
                    string url2 = store.GetRequestExecutor().Url;
                    return (url1 != url2);
                }, true);

                using (var session = store.OpenSession())
                {
                    session.Store(new User(), "users/1");
                    session.SaveChanges();
                }
                value = WaitForValue(() => list.Count, 2);
                Assert.Equal(2, value);
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

        private async Task WaitForTopologyStabilizationAsync(string s, RavenServer workingServer, int rehabCount, int memberCount)
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
                    var value = await WaitForValueAsync(() =>
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
