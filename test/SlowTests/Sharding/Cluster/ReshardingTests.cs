using System.Threading.Tasks;
using FastTests;
using FastTests.Sharding;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Utils;
using SlowTests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Sharding.Cluster
{
    public class ReshardingTests : RavenTestBase
    {
        public ReshardingTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Sharding)]
        public async Task CanMoveOneBucketManually()
        {
            // TODO: once wired, disable the observer
            using (var store = Sharding.GetDocumentStore())
            {
                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));

                var id = "foo/bar";
                using (var session = store.OpenAsyncSession())
                {
                    var user = new User
                    {
                        Name = "Original shard"
                    };
                    await session.StoreAsync(user, id);
                    await session.SaveChangesAsync();
                }

                var bucket = ShardHelper.GetBucket(id);
                var location = ShardHelper.GetShardNumber(record.ShardBucketRanges, bucket);
                var newLocation = (location + 1) % record.Shards.Length;
                using (var session = store.OpenAsyncSession(ShardHelper.ToShardName(store.Database, location)))
                {
                    var user = await session.LoadAsync<User>(id);
                    Assert.NotNull(user);
                }

                var result = await Server.ServerStore.Sharding.StartBucketMigration(store.Database, bucket, location, newLocation);

                var exists = WaitForDocument<User>(store, id, predicate: null, database: ShardHelper.ToShardName(store.Database, newLocation));
                Assert.True(exists);

                using (var session = store.OpenAsyncSession(ShardHelper.ToShardName(store.Database, newLocation)))
                {
                    var user = await session.LoadAsync<User>(id);
                    var changeVector = session.Advanced.GetChangeVectorFor(user);
                    await Server.ServerStore.Sharding.SourceMigrationCompleted(store.Database, bucket, result.Index, changeVector);
                }

                result = await Server.ServerStore.Sharding.DestinationMigrationConfirm(store.Database, bucket, result.Index);
                await Server.ServerStore.Cluster.WaitForIndexNotification(result.Index);

                // the document will be written to the new location
                using (var session = store.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>(id);
                    user.Name = "New shard";
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession(ShardHelper.ToShardName(store.Database, location)))
                {
                    var user = await session.LoadAsync<User>(id);
                    Assert.Equal("Original shard", user.Name);
                }
            }
        }

        [Fact(Skip = "Waiting for RavenDB-17760")]
        public async Task CanMoveOneBucket()
        {
            using (var store = Sharding.GetDocumentStore())
            {
                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));

                var id = "foo/bar";
                var bucket = ShardHelper.GetBucket(id);
                var location = ShardHelper.GetShardNumber(record.ShardBucketRanges, bucket);
                var newLocation = (location + 1) % record.Shards.Length;
                using (var session = store.OpenAsyncSession())
                {
                    var user = new User
                    {
                        Name = "Original shard"
                    };
                    await session.StoreAsync(user, id);
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession(ShardHelper.ToShardName(store.Database, location)))
                {
                    var user = await session.LoadAsync<User>(id);
                    Assert.NotNull(user);
                }

                var exists = WaitForDocument<User>(store, id, predicate: null, database: ShardHelper.ToShardName(store.Database, newLocation));
                Assert.True(exists);

                using (var session = store.OpenAsyncSession(ShardHelper.ToShardName(store.Database, newLocation)))
                {
                    var user = await session.LoadAsync<User>(id);
                    var changeVector = session.Advanced.GetChangeVectorFor(user);
                    //await Server.ServerStore.Sharding.SourceMigrationCompleted(store.Database, bucket, result.Index, changeVector);
                }

                //await Server.ServerStore.Sharding.DestinationMigrationConfirm(store.Database, bucket, result.Index);

                // the document will be written to the new location
                using (var session = store.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>(id);
                    user.Name = "New shard";
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession(ShardHelper.ToShardName(store.Database, location)))
                {
                    var user = await session.LoadAsync<User>(id);
                    Assert.Equal("Original shard", user.Name);
                }
            }
        }
    }
}
