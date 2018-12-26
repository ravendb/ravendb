using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Commands;
using Raven.Client.Exceptions;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server;
using Raven.Server.Config;
using Raven.Server.Utils;
using Tests.Infrastructure;
using Raven.Tests.Core.Utils.Entities;
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
            DebuggerAttachedTimeout.DisableLongTimespan = true;

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

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User(), "users/1");
                    await session.SaveChangesAsync();
                }

                Assert.Equal(list.Count, 1);

                string currnetUrl = store.GetRequestExecutor().Url;
                RavenServer toDispose = null;
                foreach (var server in Servers)
                {
                    if (server.WebUrl == currnetUrl)
                    {
                        toDispose = server;
                        break;
                    }
                }

                DisposeServerAndWaitForFinishOfDisposal(toDispose);
                await taskObservable.EnsureConnectedNow();

                await Task.Delay(12000);

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User(), "users/1");
                    await session.SaveChangesAsync();
                }
                Assert.Equal(list.Count, 2);

                currnetUrl = store.GetRequestExecutor().Url;
                foreach (var server in Servers)
                {
                    if (server.WebUrl == currnetUrl)
                    {
                        toDispose = server;
                        break;
                    }
                }

                DisposeServerAndWaitForFinishOfDisposal(toDispose);
                await taskObservable.EnsureConnectedNow();

                await Task.Delay(500);

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User(), "users/1");
                    await session.SaveChangesAsync();
                }

                Assert.Equal(list.Count, 3);
            }
        }

        [Fact]
        public async Task ChangesApiReorderDatabaseNodes()
        {
            DebuggerAttachedTimeout.DisableLongTimespan = true;

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

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User(), "users/1");
                    await session.SaveChangesAsync();
                }

                await Task.Delay(100);
                Assert.Equal(list.Count, 1);

                string url1 = store.GetRequestExecutor().Url;
                await ReverseOrderSuccessfully(store, db);

                await Task.Delay(500);

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User(), "users/1");
                    await session.SaveChangesAsync();
                }
                Assert.Equal(list.Count, 2);
                string url2 = store.GetRequestExecutor().Url;
                Assert.NotEqual(url1, url2);
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
