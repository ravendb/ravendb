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
using Raven.Client.ServerWide;
using Raven.Server.Rachis;
using Raven.Server.ServerWide.Commands;
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

        [RavenFact(RavenTestCategory.Cluster)]
        public async Task Handle_Outdated_Leader_On_RachisMergedCommand()
        {
            var (nodes, leader) = await CreateRaftCluster(numberOfNodes: 2, watcherCluster: true);
            var follower = nodes.First(n => n.ServerStore.NodeTag != leader.ServerStore.NodeTag);

            using var store = GetDocumentStore(new Options()
            {
                Server = leader,
                ReplicationFactor = nodes.Count
            });
            using var followerStore = new DocumentStore
            {
                Database = store.Database, 
                Urls = new[] { follower.WebUrl }, 
                Conventions = new DocumentConventions { DisableTopologyUpdates = true }
            }.Initialize();

            // Fail RachisMergedCommand InsertToLeaderLog on execute once
            var first = 0;
            leader.ServerStore.Engine.ForTestingPurposesOnly().ModifyTermBeforeRachisMergedCommandInsertToLeaderLog = (command, term) =>
            {
                if (command is ClusterTransactionCommand &&
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

        [RavenFact(RavenTestCategory.Cluster)]
        public async Task RachisConcurrencyException_On_Leader_PutAsync_Shouldnt_Make_Endless_Loop_On_SendToLeaderAsync()
        {
            var (nodes, leader) = await CreateRaftCluster(2, watcherCluster: true);

            var follower = nodes.First(n => n.ServerStore.NodeTag != leader.ServerStore.NodeTag);

            leader.ServerStore.Engine.ForTestingPurposesOnly().BeforeExecuteAddDatabaseCommand = () => throw new RachisConcurrencyException("For Testing");

            // Should be ConcurrencyException, not TimeoutException (in SendToLeaderAsync)
            var db = GetDatabaseName();
            await Assert.ThrowsAsync<ConcurrencyException>( () => 
                follower.ServerStore.SendToLeaderAsync(new AddDatabaseCommand(Guid.NewGuid().ToString())
                {
                    Record = new DatabaseRecord(db) { Topology = new DatabaseTopology { Members = new List<string> { "A", "B" } } }, Name = db
                })
            );
        }

        private class User
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }
    }
}
