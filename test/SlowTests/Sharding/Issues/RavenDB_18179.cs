using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.ServerWide.Operations;
using Raven.Client.Util;
using Raven.Server.Utils;
using Raven.Tests.Core.Utils.Entities;
using SlowTests.Core.Utils.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Sharding.Issues;

public class RavenDB_18179 : ClusterTestBase
{
    public RavenDB_18179(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Sharding | RavenTestCategory.Querying)]
    public async Task ShouldMarkQueryResultsAsStaleIfBucketsAreBeingMigrated()
    {
        DoNotReuseServer();
        using (var store = Sharding.GetDocumentStore())
        {
            Server.ServerStore.Sharding.ManualMigration = true;

            var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));

            var id = "foo/bar";
            using (var session = store.OpenAsyncSession())
            {
                var user = new User
                {
                    Name = "Arek"
                };
                await session.StoreAsync(user, id);
                await session.SaveChangesAsync();
            }

            new Users_ByName().Execute(store);

            Indexes.WaitForIndexing(store);

            var bucket = Sharding.GetBucket(record.Sharding, id);
            var shardNumber = ShardHelper.GetShardNumberFor(record.Sharding, bucket);
            var toShard = ShardingTestBase.GetNextSortedShardNumber(record.Sharding.Shards, shardNumber);
            using (var session = store.OpenAsyncSession(ShardHelper.ToShardName(store.Database, shardNumber)))
            {
                var user = await session.LoadAsync<User>(id);
                Assert.NotNull(user);
            }

            var result = await Server.ServerStore.Sharding.StartBucketMigration(store.Database, bucket, toShard, RaftIdGenerator.NewId());

            var exists = WaitForDocument<User>(store, id, predicate: null, database: ShardHelper.ToShardName(store.Database, toShard));
            Assert.True(exists);

            using (var session = store.OpenSession())
            {
                List<User> users = session.Query<User, Users_ByName>().Statistics(out var stats).ToList();

                Assert.True(stats.IsStale);
            }

            using (var session = store.OpenAsyncSession(ShardHelper.ToShardName(store.Database, toShard)))
            {
                var user = await session.LoadAsync<User>(id);
                var changeVector = session.Advanced.GetChangeVectorFor(user);
                await Server.ServerStore.Sharding.SourceMigrationCompleted(store.Database, bucket, result.Index, changeVector, RaftIdGenerator.NewId());
            }

            result = await Server.ServerStore.Sharding.DestinationMigrationConfirm(store.Database, bucket, result.Index);
            await Server.ServerStore.Cluster.WaitForIndexNotification(result.Index);

            using (var session = store.OpenSession())
            {
                List<User> users = session.Query<User, Users_ByName>().Statistics(out var stats).ToList();

                Assert.False(stats.IsStale);
            }
        }
    }

    private class Users_ByName : AbstractIndexCreationTask<User>
    {
        public Users_ByName()
        {
            Map = users => from u in users select new {u.Name};
        }
    }
}
