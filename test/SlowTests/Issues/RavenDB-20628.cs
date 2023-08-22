using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using FastTests.Graph;
using Raven.Client.Documents;
using Raven.Client.Documents.Session;
using Raven.Server;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_20628 : ClusterTestBase
    {
        public RavenDB_20628(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task RequestExecutor_With_CanellationToken_Should_Throw_In_Timeout_When_ClusterWideTransaction_Is_Slow()
        {
            using var store = GetDocumentStore();

            var user1 = new User()
            {
                Id = "Users/1-A",
                Name = "Alice"
            };


            using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                await session.StoreAsync(user1);
                await session.SaveChangesAsync();
            }

            using var cts = new CancellationTokenSource();

            var db = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
            db.ForTestingPurposesOnly().AfterCommitInClusterTransaction = () =>
            {
                cts.Cancel();
            };

            var e = await Assert.ThrowsAsync<TaskCanceledException>(async () =>
            {
                using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    var user2 = await session.LoadAsync<User>(user1.Id);
                    user2.Name = "Bob";
                    await session.SaveChangesAsync(cts.Token);
                }
            });

            Assert.NotNull(e);
        }

        [Fact]
        public async Task ClusterTransaction_Failover_Shouldnt_Throw_ConcurrencyException()
        {
            var (nodes, leader) = await CreateRaftCluster(numberOfNodes: 3);
            using var store = GetDocumentStore(new Options
            {
                Server = leader,
                ReplicationFactor = nodes.Count
            });
            var databaseName = store.Database;

            var disposeNodeTask = Task.Run(async () =>
            {
                await Task.Delay(400);
                var tag = store.GetRequestExecutor(databaseName).TopologyNodes.First().ClusterTag;
                var server = nodes.Single(n => n.ServerStore.NodeTag == tag);
                await DisposeServerAndWaitForFinishOfDisposalAsync(server);
            });
            await ProcessDocument(store, "Docs/1-A");

            await disposeNodeTask;
        }

        private async Task ProcessDocument(IDocumentStore store, string id)
        {
            using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                var doc = new Doc { Id = id};
                await session.StoreAsync(doc);
                await session.SaveChangesAsync();
            }

            for (int i = 0; i < 2000; i++)
            {
                using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    var doc = await session.LoadAsync<Doc>(id);
                    doc.Progress = i;
                    await session.SaveChangesAsync();
                }
            }
        }

        public class Doc
        {
            public string Id { get; set; }
            public int Progress { get; set; }
        }

        [Fact]
        public async Task ClusterTransaction_Should_Work_After_Commit_And_Failover()
        {
            var (nodes, leader) = await CreateRaftCluster(numberOfNodes: 2, watcherCluster: true);
            using var store = GetDocumentStore(new Options
            {
                Server = leader,
                ReplicationFactor = nodes.Count
            });

            await ApplyFailoverAfterCommit(nodes, store.Database);

            var user1 = new User() { Id = "Users/1-A", Name = "Alice" };
            using (var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15)))
            using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                await session.StoreAsync(user1);
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                var users1onSession = await session.LoadAsync<User>(user1.Id);
                Assert.Equal(users1onSession.Name, "Alice");
            }
        }

        [Fact]
        public async Task ClusterTransaction_WithMultipleCommands_Should_Work_After_Commit_And_Failover()
        {
            var (nodes, leader) = await CreateRaftCluster(numberOfNodes: 2, watcherCluster: true);
            using var store = GetDocumentStore(new Options
            {
                Server = leader,
                ReplicationFactor = nodes.Count
            });

            await ApplyFailoverAfterCommit(nodes, store.Database);

            var user1 = new User() { Id = "Users/1-A", Name = "Alice" };
            var user2 = new User() { Id = "Users/2-A", Name = "Bob" };
            var user3 = new User() { Id = "Users/3-A", Name = "Alice" };

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(user1);
                await session.StoreAsync(user3);
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                (await session.LoadAsync<User>(user1.Id)).Name = "Bob";
                await session.StoreAsync(user2);
                session.Delete(user3.Id);
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                var users1onSession = await session.LoadAsync<User>(user1.Id);
                Assert.Equal(users1onSession.Name, "Bob");

                var users2onSession = await session.LoadAsync<User>(user2.Id);
                Assert.Equal(users2onSession.Name, "Bob");

                var users3onSession = await session.LoadAsync<User>(user3.Id);
                Assert.Null(users3onSession);
            }
        }

        [Fact]
        public async Task ClusterTransaction_WithMultipleCommands_Should_Work_After_Commit_And_Failover_UseResults()
        {
            var (nodes, leader) = await CreateRaftCluster(numberOfNodes: 2, watcherCluster: true);
            using var store = GetDocumentStore(new Options
            {
                Server = leader,
                ReplicationFactor = nodes.Count
            });

            await ApplyFailoverAfterCommit(nodes, store.Database);

            var user1 = new User() { Id = "Users/1-A", Name = "Alice" };
            var user2 = new User() { Id = "Users/2-A", Name = "Bob" };
            var user3 = new User() { Id = "Users/3-A", Name = "Alice" };

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(user1);
                await session.StoreAsync(user3);
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                var users1onSession = await session.LoadAsync<User>(user1.Id);
                users1onSession.Name = "Bob";
                await session.StoreAsync(user2);
                session.Delete(user3.Id);
                await session.SaveChangesAsync();

                users1onSession.Name = "Shahar";
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                var users1onSession = await session.LoadAsync<User>(user1.Id);
                Assert.Equal(users1onSession.Name, "Shahar");

                var users2onSession = await session.LoadAsync<User>(user2.Id);
                Assert.Equal(users2onSession.Name, "Bob");

                var users3onSession = await session.LoadAsync<User>(user3.Id);
                Assert.Null(users3onSession);
            }
        }

        private async Task ApplyFailoverAfterCommit(List<RavenServer> nodes, string database)
        {
            int failover = 0;
            foreach (var n in nodes)
            {
                var server = n;
                var db = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
                db.ForTestingPurposesOnly().AfterCommitInClusterTransaction = () =>
                {
                    if (Interlocked.CompareExchange(ref failover, 1, 0) == 0)
                        throw new TimeoutException("Shahar"); // for failover in node A
                };
            }
        }

    }
}


