using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_22345 : ClusterTestBase
    {
        public RavenDB_22345(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Cluster)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task HandleOutdatedLeaderOnRachisMergedCommand(Options options)
        {
            var (nodes, leader) = await CreateRaftCluster(numberOfNodes: 2, watcherCluster: true);
            var follower = nodes.First(n => n.ServerStore.NodeTag != leader.ServerStore.NodeTag);

            options.Server = leader;
            options.ReplicationFactor = nodes.Count;
            using var store = GetDocumentStore(options);
            using var followerStore = new DocumentStore
            {
                Database = store.Database, Urls = new[] { follower.WebUrl }, Conventions = new DocumentConventions { DisableTopologyUpdates = true }
            }.Initialize();

            // Fail RachisMergedCommand InsertToLeaderLog on execute once
            var first = 0;
            leader.ServerStore.ForTestingPurposesOnly().ModifyTermBeforeRachisMergedCommandInsertToLeaderLog = (command, term) =>
            {
                if (command is Raven.Server.ServerWide.Commands.ClusterTransactionCommand &&
                    Interlocked.CompareExchange(ref first, 1, 0) == 0)
                    term--;
                return term;
            };

            var user1 = new User() { Id = "Users/1-A", Name = "Alice" };
            using (var session = followerStore.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                await session.StoreAsync(user1);
                await session.SaveChangesAsync(); // Fail: throws ConcurrencyException
            }


            using (var session = store.OpenAsyncSession())
            {
                var u1 = await session.LoadAsync<User>(user1.Id);
                Assert.NotNull(u1);
            }
        }

        private class User
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }
    }
}
