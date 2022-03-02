using System;
using System.Threading.Tasks;
using FastTests.Sharding;
using Raven.Client.Documents.Operations;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Utils;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Sharding
{
    public class ReshardingTests : ShardedTestBase
    {
        public ReshardingTests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task CanMoveOneBucketManually()
        {
            // TODO: once wired, disable the observer
            using (var store = GetShardedDocumentStore())
            {
                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));

                var id = "foo/bar";
                var bucket = ShardHelper.GetBucket(id);
                var location = ShardHelper.GetShardIndex(record.ShardAllocations, bucket);
                var newLocation = (location + 1) % record.Shards.Length;
                using (var session = store.OpenAsyncSession())
                {
                    var user = new BasicSharding.User
                    {
                        Name = "Original shard"
                    };
                    await session.StoreAsync(user, id);
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession(ShardHelper.ToShardName(store.Database, location)))
                {
                    var user = await session.LoadAsync<BasicSharding.User>(id);
                    Assert.NotNull(user);
                }

                var result = await Server.ServerStore.Sharding.StartBucketMigration(store.Database, bucket, location, newLocation);

                var exists = WaitForDocument<BasicSharding.User>(store, id, predicate: null, database: ShardHelper.ToShardName(store.Database, newLocation));
                Assert.True(exists);

                using (var session = store.OpenAsyncSession(ShardHelper.ToShardName(store.Database, newLocation)))
                {
                    var user = await session.LoadAsync<BasicSharding.User>(id);
                    var changeVector = session.Advanced.GetChangeVectorFor(user);
                    await Server.ServerStore.Sharding.SourceMigrationCompleted(store.Database, bucket, result.Index, changeVector);
                }

                result = await Server.ServerStore.Sharding.DestinationMigrationConfirm(store.Database, bucket, result.Index);
                await Server.ServerStore.Cluster.WaitForIndexNotification(result.Index);

                // the document will be written to the new location
                using (var session = store.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<BasicSharding.User>(id);
                    user.Name = "New shard";
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession(ShardHelper.ToShardName(store.Database, location)))
                {
                    var user = await session.LoadAsync<BasicSharding.User>(id);
                    Assert.Equal("Original shard", user.Name);
                }
            }
        }

        [Fact(Skip = "Waiting for RavenDB-17760")]
        public async Task CanMoveOneBucket()
        {
            using (var store = GetShardedDocumentStore())
            {
                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));

                var id = "foo/bar";
                var bucket = ShardHelper.GetBucket(id);
                var location = ShardHelper.GetShardIndex(record.ShardAllocations, bucket);
                var newLocation = (location + 1) % record.Shards.Length;
                using (var session = store.OpenAsyncSession())
                {
                    var user = new BasicSharding.User
                    {
                        Name = "Original shard"
                    };
                    await session.StoreAsync(user, id);
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession(ShardHelper.ToShardName(store.Database, location)))
                {
                    var user = await session.LoadAsync<BasicSharding.User>(id);
                    Assert.NotNull(user);
                }

                var exists = WaitForDocument<BasicSharding.User>(store, id, predicate: null, database: ShardHelper.ToShardName(store.Database, newLocation));
                Assert.True(exists);

                using (var session = store.OpenAsyncSession(ShardHelper.ToShardName(store.Database, newLocation)))
                {
                    var user = await session.LoadAsync<BasicSharding.User>(id);
                    var changeVector = session.Advanced.GetChangeVectorFor(user);
                    //await Server.ServerStore.Sharding.SourceMigrationCompleted(store.Database, bucket, result.Index, changeVector);
                }

                //await Server.ServerStore.Sharding.DestinationMigrationConfirm(store.Database, bucket, result.Index);

                // the document will be written to the new location
                using (var session = store.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<BasicSharding.User>(id);
                    user.Name = "New shard";
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession(ShardHelper.ToShardName(store.Database, location)))
                {
                    var user = await session.LoadAsync<BasicSharding.User>(id);
                    Assert.Equal("Original shard", user.Name);
                }
            }
        }

        [Fact]
        public async Task CanGetShardedDatabaseStats()
        {
            using (var store = GetShardedDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    for (var i = 0; i < 10; i++)
                    {
                        var id = $"foo/bar/{i}";
                        var user = new BasicSharding.User
                        {
                            Name = "Original shard"
                        };
                        await session.StoreAsync(user, id);
                        await session.SaveChangesAsync();

                        var baseline = DateTime.Today;
                        var ts = session.TimeSeriesFor(id, "HeartRates");
                        var cf = session.CountersFor(id);
                        for (var j = 0; j < 20; j++)
                        {
                            ts.Append(baseline.AddMinutes(j), j, "watches/apple");
                            cf.Increment("Likes", j);
                        }

                        await session.SaveChangesAsync();
                    }
                }

                var databaseStatistics = await store.Maintenance.SendAsync(new GetStatisticsOperation());

                Assert.NotNull(databaseStatistics);
                Assert.Equal(10, databaseStatistics.CountOfDocuments);
                Assert.Equal(10, databaseStatistics.CountOfCounterEntries);
                Assert.Equal(10, databaseStatistics.CountOfTimeSeriesSegments);

                using (var session = store.OpenAsyncSession())
                {
                    session.Delete("foo/bar/0");
                    await session.SaveChangesAsync();
                }

                databaseStatistics = await store.Maintenance.SendAsync(new GetStatisticsOperation());

                Assert.Equal(9, databaseStatistics.CountOfDocuments);
                Assert.Equal(1, databaseStatistics.CountOfTombstones);
            }
        }
    }
}
