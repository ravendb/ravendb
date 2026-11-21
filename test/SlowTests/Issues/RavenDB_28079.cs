using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Indexes;
using Raven.Client.Exceptions.Database;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server;
using Raven.Server.Config;
using Raven.Server.Documents;
using Raven.Server.Documents.Indexes;
using Raven.Server.ServerWide.Commands.Indexes;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_28079 : ClusterTestBase
    {
        public RavenDB_28079(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Cluster | RavenTestCategory.Indexes)]
        public void RollingIndexDeploymentDoesNotAddNodesThatJoinedAfterItStarted()
        {
            var createdAt = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);

            var record = CreateRecordWithRollingIndex(createdAt);
            new PutRollingIndexCommand(record.DatabaseName, "Users/ByName", "A", createdAt.AddMinutes(5), "finish").UpdateDatabaseRecord(record, 10);
            Assert.Equal(["A", "B"], record.RollingIndexes["Users/ByName"].ActiveDeployments.Keys.Order());

            record = CreateRecordWithRollingIndex(createdAt);
            new PutRollingIndexCommand(record.DatabaseName, "Users/ByName", "A", finishedAt: null, "dummy update").UpdateDatabaseRecord(record, 10);
            Assert.Equal(["A", "B"], record.RollingIndexes["Users/ByName"].ActiveDeployments.Keys.Order());
        }

        [RavenFact(RavenTestCategory.Cluster | RavenTestCategory.Indexes)]
        public async Task NodeAddedDuringRollingDeploymentDoesNotWaitForItsTurn()
        {
            var (nodes, leader) = await CreateRaftCluster(3, watcherCluster: true);
            using var store = GetDocumentStore(new Options
            {
                Server = leader,
                ReplicationFactor = 2,
                ModifyDatabaseRecord = record => record.Settings[RavenConfiguration.GetKey(x => x.Indexing.StaticIndexDeploymentMode)] = nameof(IndexDeploymentMode.Rolling)
            });

            using (var session = store.OpenAsyncSession())
            {
                for (var i = 0; i < 10; i++)
                    await session.StoreAsync(new User { Name = "user/" + i });

                await session.SaveChangesAsync();
            }

            var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
            using var hold = new ManualResetEventSlim();

            try
            {
                foreach (var node in nodes.Where(x => record.Topology.RelevantFor(x.ServerStore.NodeTag)))
                {
                    var database = await node.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
                    database.IndexStore.ForTestingPurposesOnly().OnRollingIndexStart = _ => hold.Wait(database.DatabaseShutdown);
                }

                await new UsersByName().ExecuteAsync(store);
                await store.Maintenance.Server.SendAsync(new AddDatabaseNodeOperation(store.Database));

                var newNode = nodes.Single(x => record.Topology.RelevantFor(x.ServerStore.NodeTag) == false);
                var indexed = await WaitForValueAsync(() => HasIndexedDocuments(newNode, store.Database, nameof(UsersByName)), true, timeout: 30_000);
                Assert.True(indexed);
            }
            finally
            {
                hold.Set();
            }
        }

        private static async Task<bool> HasIndexedDocuments(RavenServer server, string databaseName, string indexName)
        {
            DocumentDatabase database;
            try
            {
                database = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(databaseName);
            }
            catch (DatabaseDoesNotExistException)
            {
                return false;
            }

            var index = database?.IndexStore.GetIndex(indexName);
            if (index == null)
                return false;

            using (var context = QueryOperationContext.Allocate(database, index))
            using (context.OpenReadTransaction())
                return index.GetIndexingState(context).LastProcessedEtag > 0;
        }

        private class UsersByName : AbstractIndexCreationTask<User>
        {
            public UsersByName()
            {
                Map = users => from user in users
                               select new { user.Name };
            }
        }

        [RavenFact(RavenTestCategory.Cluster | RavenTestCategory.Indexes)]
        public void FinishOfAnOlderRollingDeploymentIsIgnored()
        {
            var createdAt = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);

            var record = CreateRecordWithRollingIndex(createdAt);
            new PutRollingIndexCommand(record.DatabaseName, "Users/ByName", "A", createdAt, "stale") { DeploymentRaftCommandIndex = 4 }.UpdateDatabaseRecord(record, 10);
            Assert.Equal(RollingIndexState.Running, record.RollingIndexes["Users/ByName"].ActiveDeployments["A"].State);

            new PutRollingIndexCommand(record.DatabaseName, "Users/ByName", "A", createdAt, "current") { DeploymentRaftCommandIndex = 5 }.UpdateDatabaseRecord(record, 11);
            Assert.Equal(RollingIndexState.Done, record.RollingIndexes["Users/ByName"].ActiveDeployments["A"].State);
        }

        private static DatabaseRecord CreateRecordWithRollingIndex(DateTime createdAt)
        {
            var record = new DatabaseRecord("db")
            {
                Topology = new DatabaseTopology
                {
                    Members = new List<string> { "A", "B", "C" }
                },
                RollingIndexes = new Dictionary<string, RollingIndex>()
            };

            var rollingIndex = new RollingIndex { RaftCommandIndex = 5 };
            rollingIndex.ActiveDeployments["A"] = new RollingIndexDeployment
            {
                State = RollingIndexState.Running,
                CreatedAt = createdAt,
                StartedAt = createdAt
            };
            rollingIndex.ActiveDeployments["B"] = new RollingIndexDeployment
            {
                State = RollingIndexState.Pending,
                CreatedAt = createdAt
            };
            record.RollingIndexes["Users/ByName"] = rollingIndex;

            return record;
        }
    }
}
