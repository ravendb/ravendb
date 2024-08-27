using System;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations;
using Raven.Client.Exceptions;
using Raven.Client.Http;
using Raven.Client.Json.Serialization;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Sharding;
using Raven.Server.Utils;
using SlowTests.Core.Utils.Entities;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Sharding.Issues
{
    public class RavenDB_22731 : RavenTestBase
    {
        public RavenDB_22731(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Sharding)]
        public async Task ReshardingEndpoint_ShouldThrowOnAttemptToMoveBucketFromPrefixedRange_ToShardNotInPrefixSetting()
        {
            using var store = Sharding.GetDocumentStore(new Options
            {
                ModifyDatabaseRecord = record =>
                {
                    record.Sharding ??= new ShardingConfiguration();
                    record.Sharding.Prefixed =
                    [
                        new PrefixedShardingSetting
                        {
                            Prefix = "users/",
                            Shards = [0]
                        }
                    ];
                }
            });
            {
                const string id = "users/1";
                const int destShard = 2; // not in prefix setting

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User(), id);
                    await session.SaveChangesAsync();
                }

                var bucket = await Sharding.GetBucketAsync(store, id);

                using (var session = store.OpenAsyncSession())
                using (var context = JsonOperationContext.ShortTermSingleUse())
                {
                    var command = new StartReshardingCommand(store.Database, fromBucket: bucket, toBucket: bucket, destinationShard: destShard);
                    await session.Advanced.RequestExecutor.ExecuteAsync(command, context);

                    var op = new ServerWideOperation(session.Advanced.RequestExecutor, store.Conventions, command.Result.OperationId, command.Result.OperationNodeTag);

                    var ex = await Assert.ThrowsAsync<RavenException>(async () => await op.WaitForCompletionAsync(TimeSpan.FromSeconds(60)));

                    Assert.Contains($"Failed to start migration of bucket '{bucket}'. Destination shard {destShard} doesn't exist", ex.Message);
                }
            }
        }

        [RavenFact(RavenTestCategory.Sharding)]
        public async Task ReshardingEndpoint_CanMoveSingleBucket_FromPrefixedRange()
        {
            using var store = Sharding.GetDocumentStore(new Options
            {
                ModifyDatabaseRecord = record =>
                {
                    record.Sharding ??= new ShardingConfiguration();
                    record.Sharding.Prefixed =
                    [
                        new PrefixedShardingSetting
                        {
                            Prefix = "users/",
                            Shards = [0, 2]
                        }
                    ];
                }
            });
            {
                const string id = "users/1";
                const int sourceShard = 2;
                const int destShard = 0;

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User(), id);
                    await session.SaveChangesAsync();
                }

                var shardNum = await Sharding.GetShardNumberForAsync(store, id);
                Assert.Equal(sourceShard, shardNum);

                var bucket = await Sharding.GetBucketAsync(store, id);

                using (var session = store.OpenAsyncSession())
                using (var context = JsonOperationContext.ShortTermSingleUse())
                {
                    // prefix string is not specified in StartReshardingCommand
                    // this should be handled on the server side - recognize that this bucket belongs to prefixed range, find the 
                    // matching prefix setting, and verify that we have a valid destination shard

                    var command = new StartReshardingCommand(store.Database, fromBucket: bucket, toBucket: bucket, destinationShard: destShard);
                    await session.Advanced.RequestExecutor.ExecuteAsync(command, context);
                }

                await WaitForValueAsync(async () =>
                {
                    return shardNum = await Sharding.GetShardNumberForAsync(store, id);
                }, expectedVal: destShard, timeout: 60_000);

                Assert.Equal(destShard, shardNum);
            }
        }

        [RavenFact(RavenTestCategory.Sharding, Skip = "takes too long")]
        public async Task ReshardingEndpoint_CanMoveRangeOfBuckets_FromPrefixed()
        {
            using var store = Sharding.GetDocumentStore(new Options
            {
                ModifyDatabaseRecord = record =>
                {
                    record.Sharding ??= new ShardingConfiguration();
                    record.Sharding.Prefixed =
                    [
                        new PrefixedShardingSetting
                        {
                            Prefix = "users/",
                            Shards = [0, 2]
                        }
                    ];
                }
            });
            {
                const int destShard = 2;

                using (var session = store.OpenAsyncSession())
                {
                    for (int i = 0; i < 100; i++)
                    {
                        await session.StoreAsync(new User(), $"users/{i}");
                    }

                    await session.SaveChangesAsync();
                }

                int userDocsInShard0 = -1;
                using (var session = store.OpenSession(ShardHelper.ToShardName(store.Database, 0)))
                {
                    userDocsInShard0 = session.Query<User>().Count();
                    Assert.True(userDocsInShard0 > 0);
                }

                var fromBucket = ShardHelper.NumberOfBuckets;
                var toBucket = (int)(ShardHelper.NumberOfBuckets * 1.5);

                using (var session = store.OpenAsyncSession())
                using (var context = JsonOperationContext.ShortTermSingleUse())
                {
                    var command = new StartReshardingCommand(store.Database, fromBucket, toBucket, destinationShard: destShard);
                    await session.Advanced.RequestExecutor.ExecuteAsync(command, context);

                    await WaitForValueAsync(() =>
                    {
                        using (var session = store.OpenSession(ShardHelper.ToShardName(store.Database, 0)))
                        {
                            //userDocsInShard0 = session.Query<User>().Count();
                            return userDocsInShard0 = session.Query<User>().Count();
                        }
                    }, expectedVal: 0, timeout: 120_000);

                    Assert.Equal(0, userDocsInShard0);
                }

                using (var session = store.OpenAsyncSession(ShardHelper.ToShardName(store.Database, 2)))
                {
                    var userDocsInShard2 = session.Query<User>().Count();
                    Assert.Equal(100, userDocsInShard2);
                }
            }
        }

        [RavenFact(RavenTestCategory.Sharding)]
        public async Task StartBucketMigrationCommand_ShouldThrowOnAttemptToMoveBucketFromPrefixedRange_IfNoPrefixStringProvided()
        {
            using var store = Sharding.GetDocumentStore(new Options
            {
                ModifyDatabaseRecord = record =>
                {
                    record.Sharding ??= new ShardingConfiguration();
                    record.Sharding.Prefixed =
                    [
                        new PrefixedShardingSetting
                        {
                            Prefix = "users/",
                            Shards = [0, 1]
                        }
                    ];
                }
            });
            {
                const string id = "users/1";
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User(), id);
                    await session.SaveChangesAsync();
                }

                var bucket = await Sharding.GetBucketAsync(store, id);

                var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () => 
                    await Server.ServerStore.Sharding.StartBucketMigration(store.Database, bucket, toShard: 1));

                Assert.Contains($"Bucket {bucket} belongs to a prefixed range, but 'prefix' parameter wasn't provided", ex.Message);
            }
        }

        private class StartReshardingCommand : RavenCommand<OperationIdResult>
        {
            private readonly string _database;
            private readonly int _fromBucket;
            private readonly int _toBucket;
            private readonly int _destinationShard;

            public StartReshardingCommand(string database, int fromBucket, int toBucket, int destinationShard)
            {
                _database = database;
                _fromBucket = fromBucket;
                _toBucket = toBucket;
                _destinationShard = destinationShard;
            }

            public override bool IsReadRequest => false;

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/resharding/start?" +
                      $"database={_database}&fromBucket={_fromBucket}&toBucket={_toBucket}&toShard={_destinationShard}";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Post
                };
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();

                var result = JsonDeserializationClient.BackupDatabaseNowResult(response);
                var operationIdResult = JsonDeserializationClient.OperationIdResult(response);

                operationIdResult.OperationNodeTag ??= result.ResponsibleNode;
                Result = operationIdResult.ForResult(result);
            }
        }
    }
}
