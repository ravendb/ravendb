using System;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server.ServerWide.Context;
using SlowTests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_15625 : ClusterTestBase
    {
        public RavenDB_15625(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task NotInRehabWithDisabledIndexes1()
        {
            var (node, leader) = await CreateRaftCluster(3, watcherCluster: true);
            var database = GetDatabaseName();
            await CreateDatabaseInClusterInner(new DatabaseRecord(database), 3, leader.WebUrl, null);
            using (var store = new DocumentStore
            {
                Database = database,
                Urls = new[] { leader.WebUrl }
            }.Initialize())
            {
                var documentDatabase = await Servers[2].ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
                var result = store.Maintenance.Send(new PutIndexesOperation(new[] {new IndexDefinition
                {
                    Maps = { "from user in docs.Users select new { user.Name }" },
                    Name = "MyIndex"
                }}));
                var indexResult = result[0];
                await WaitForRaftIndexToBeAppliedInCluster(indexResult.RaftCommandIndex, TimeSpan.FromSeconds(15));
                foreach (var server in Servers)
                { 
                    documentDatabase = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
                    var index = documentDatabase.IndexStore.GetIndex("MyIndex");
                    index.SetState(IndexState.Disabled);
                }

                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                record.Topology.Members.Remove(Servers[2].ServerStore.NodeTag);
                record.Topology.Rehabs.Add(Servers[2].ServerStore.NodeTag);
                await store.Maintenance.Server.SendAsync(new UpdateDatabaseOperation(record, record.Etag));

                var rehabs = await WaitForValueAsync(async () => await GetRehabCount(store, store.Database), 1);
                Assert.Equal(1, rehabs);

                var val = await WaitForValueAsync(async () => await GetMembersCount(store, database), 2);
                Assert.Equal(2, val);

                val = await WaitForValueAsync(async () => await GetMembersCount(store, store.Database), 3);
                Assert.Equal(3, val);

                rehabs = await WaitForValueAsync(async () => await GetRehabCount(store, store.Database), 0);
                Assert.Equal(0, rehabs);
            }
        }

        [Fact]
        public async Task NotInRehabWithDisabledIndexes2()
        {
            var (node, leader) = await CreateRaftCluster(3, watcherCluster: true);
            var database = GetDatabaseName();
            await CreateDatabaseInClusterInner(new DatabaseRecord(database), 3, leader.WebUrl, null);
            using (var store = new DocumentStore
            {
                Database = database,
                Urls = new[] { leader.WebUrl }
            }.Initialize())
            {
                var documentDatabase = await Servers[2].ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
                var result = store.Maintenance.Send(new PutIndexesOperation(new[] {new IndexDefinition
                {
                    Maps = { "from user in docs.Users select new { user.Name }" },
                    Name = "MyIndex"
                }}));
                var indexResult = result[0];
                await WaitForRaftIndexToBeAppliedInCluster(indexResult.RaftCommandIndex, TimeSpan.FromSeconds(15));
                var index = documentDatabase.IndexStore.GetIndex("MyIndex");
                index.SetState(IndexState.Disabled);

                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                record.Topology.Members.Remove(Servers[2].ServerStore.NodeTag);
                record.Topology.Rehabs.Add(Servers[2].ServerStore.NodeTag);
                await store.Maintenance.Server.SendAsync(new UpdateDatabaseOperation(record, record.Etag));

                await WaitAndAssertForValueAsync(async () => await RavenDB_15588.GetMembersAndRehabsCount(store), expectedVal: (MembersCount: 2, RehabsCount: 1));
                await WaitAndAssertForValueAsync(async () => await RavenDB_15588.GetMembersAndRehabsCount(store), expectedVal: (MembersCount: 3, RehabsCount: 0));
            }
        }

        [Fact]
        public async Task NotInRehabWithDisabledIndexes3()
        {
            var (node, leader) = await CreateRaftCluster(3, watcherCluster: true);
            var database = GetDatabaseName();
            await CreateDatabaseInClusterInner(new DatabaseRecord(database), 3, leader.WebUrl, null);
            using (var store = new DocumentStore
            {
                Database = database,
                Urls = new[] { leader.WebUrl }
            }.Initialize())
            {
                var documentDatabase = await Servers[2].ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
                var result = store.Maintenance.Send(new PutIndexesOperation(new[] {new IndexDefinition
                {
                    Maps = { "from user in docs.Users select new { user.Name }" },
                    Name = "MyIndex"
                }}));
                var indexResult = result[0];
                await WaitForRaftIndexToBeAppliedInCluster(indexResult.RaftCommandIndex, TimeSpan.FromSeconds(15));
                var index = documentDatabase.IndexStore.GetIndex("MyIndex");
                index.SetState(IndexState.Error);

                using (var session = store.OpenSession())
                {
                    session.Store(new User(){Name = "Toli"}, "user/1");
                    session.SaveChanges();
                }

                index.SetState(IndexState.Disabled);

                using (documentDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var (isStale, lastEtag) = index.GetIndexStats(context);
                    Assert.Equal(0, lastEtag);
                }

                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                record.Topology.Members.Remove(Servers[2].ServerStore.NodeTag);
                record.Topology.Rehabs.Add(Servers[2].ServerStore.NodeTag);
                await store.Maintenance.Server.SendAsync(new UpdateDatabaseOperation(record, record.Etag));

                await WaitAndAssertForValueAsync(async () => await RavenDB_15588.GetMembersAndRehabsCount(store), expectedVal: (MembersCount: 2, RehabsCount: 1));

                // assert that Servers[2] is promoted back to Member
                var expectedVal = (MembersCount: 3, RehabsCount: 0);
                var val = await WaitForPredicateAsync(async () => await RavenDB_15588.GetMembersAndRehabsCount(store), expectedVal: expectedVal);
                Assert.True(val.Equals(expectedVal), await GetDecisionsForDatabase(
                    new StringBuilder("Failed on asserting Members and Rehabs count.")
                        .AppendLine($"Expected {expectedVal}, got : {val}")));
            }
        }


        private async Task<string> GetDecisionsForDatabase(StringBuilder sb)
        {
            await ActionWithLeader(l =>
            {
                var dbDecisions = l.ServerStore.Observer.ReadDecisionsForDatabase();

                sb.AppendLine("Decisions for database:");
                sb.AppendLine(string.Join(',', dbDecisions.List.Select(d => d.ToString())));
            });

            return sb.ToString();
        }

        private static async Task<T> WaitForPredicateAsync<T>(Func<Task<T>> act, T expectedVal)
        {
            var val = await WaitForPredicateAsync(t => t.Equals(expectedVal), act, timeout: 15000, interval: 100);
            return val;
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

        private static async Task<int> GetRehabCount(IDocumentStore store, string databaseName)
        {
            var res = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(databaseName));
            if (res == null)
            {
                return -1;
            }
            return res.Topology.Rehabs.Count;
        }
    }
}
