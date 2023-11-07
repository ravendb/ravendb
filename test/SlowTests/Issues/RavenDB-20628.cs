using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Session;
using Raven.Server;
using SlowTests.Core.Utils.Entities;
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

        [RavenFact(RavenTestCategory.Cluster)]
        public async Task ClusterTransaction_Should_Work_After_Commit_And_Failover()
        {
            var (nodes, leader) = await CreateRaftCluster(numberOfNodes: 2, watcherCluster: true);
            using var store = GetDocumentStore(new Options
            {
                Server = leader,
                ReplicationFactor = nodes.Count
            });

            await ApplyFailoverAfterCommitAsync(nodes, store.Database);

            var user1 = new User() { Id = "Users/1-A", Name = "Alice" };
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

        [RavenFact(RavenTestCategory.Cluster)]
        public async Task ClusterTransaction_WithMultipleCommands_Should_Work_After_Commit_And_Failover()
        {
            var (nodes, leader) = await CreateRaftCluster(numberOfNodes: 2, watcherCluster: true);
            using var store = GetDocumentStore(new Options
            {
                Server = leader,
                ReplicationFactor = nodes.Count
            });

            await ApplyFailoverAfterCommitAsync(nodes, store.Database);

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

        [RavenFact(RavenTestCategory.Cluster)]
        public async Task ClusterTransaction_WithMultipleCommands_Should_Work_After_Commit_And_Failover_UseResults()
        {
            var (nodes, leader) = await CreateRaftCluster(numberOfNodes: 2, watcherCluster: true);
            using var store = GetDocumentStore(new Options
            {
                Server = leader,
                ReplicationFactor = nodes.Count
            });

            await ApplyFailoverAfterCommitAsync(nodes, store.Database);

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

        private async Task ApplyFailoverAfterCommitAsync(List<RavenServer> nodes, string database)
        {
            int failover = 0;
            foreach (var n in nodes)
            {
                var server = n;
                var db = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
                db.ForTestingPurposesOnly().AfterCommitInClusterTransaction = () =>
                {
                    if (Interlocked.CompareExchange(ref failover, 1, 0) == 0)
                        throw new TimeoutException("Fake server fail that cause failover"); // for failover in node A
                };
            }
        }

    }
}
