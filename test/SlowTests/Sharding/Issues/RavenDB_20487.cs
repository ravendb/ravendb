using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests.Utils;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Smuggler;
using Raven.Server;
using Raven.Server.Config;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Sharding.Issues
{
    public class RavenDB_20487 : ReplicationTestBase
    {
        public RavenDB_20487(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Replication | RavenTestCategory.Sharding)]
        public async Task ReplicationToShardedAndThenToNonShardedShouldWork()
        {
            using (var store1 = GetDocumentStore())
            using (var store2 = Sharding.GetDocumentStore(new Options()
            {
                ModifyDatabaseRecord = record => record.Settings[RavenConfiguration.GetKey(x => x.Replication.MaxItemsCount)] = "10"
            }))
            using (var store3 = GetDocumentStore())
            {
                var count = 100;
                for (int i = 0; i < count; i++)
                {
                    using (var session = store1.OpenSession())
                    {
                        session.Store(new User() { Age = i }, $"Users/{i}");
                        session.SaveChanges();
                    }
                }

                await SetupReplicationAsync(store1, store2);

                var res = WaitForValue(() =>
                {
                    using (var session = store2.OpenSession())
                    {
                        return session.Query<User>().Count();
                    }
                }, count, timeout: 60_000, interval: 333);

                Assert.Equal(count, res);

                await SetupReplicationAsync(store2, store3);

                res = WaitForValue(() =>
                {
                    using (var session = store3.OpenSession())
                    {
                        return session.Query<User>().Count();
                    }
                }, count, timeout: 60_000, interval: 333);

                Assert.Equal(count, res);
            }
        }

        [RavenFact(RavenTestCategory.Replication | RavenTestCategory.Revisions | RavenTestCategory.Sharding)]
        public async Task ReplicationWithRevisionsToShardedAndThenToNonShardedShouldWork()
        {
            using (var store1 = GetDocumentStore())
            using (var store2 = Sharding.GetDocumentStore(new Options()
            {
                ModifyDatabaseRecord = record => record.Settings[RavenConfiguration.GetKey(x => x.Replication.MaxItemsCount)] = "10"
            }))
            using (var store3 = GetDocumentStore())
            {
                await RevisionsHelper.SetupRevisionsAsync(store1);

                var count = 100;
                for (int i = 0; i < count; i++)
                {
                    using (var session = store1.OpenSession())
                    {
                        session.Store(new User() { Age = i }, $"Users/{i}");
                        session.SaveChanges();
                    }
                }

                // create revisions
                for (int i = 0; i < count; i++)
                {
                    using (var session = store1.OpenSession())
                    {
                        var user = session.Load<User>($"Users/{i}");
                        Assert.NotNull(user);
                        user.Age = i + 1;
                        session.SaveChanges();
                    }
                }

                await SetupReplicationAsync(store1, store2);

                var res = WaitForValue(() =>
                {
                    using (var session = store2.OpenSession())
                    {
                        return session.Query<User>().Count();
                    }
                }, count, timeout: 60_000, interval: 333);

                Assert.Equal(count, res);

                var expectedRevisionsCount = count * 2;
                res = await WaitForValueAsync(async () =>
                {
                    var stats = await GetDatabaseStatisticsAsync(store2);
                    return (int)stats.CountOfRevisionDocuments;
                }, expectedRevisionsCount, timeout: 30_000, interval: 333);

                Assert.Equal(expectedRevisionsCount, res);

                await SetupReplicationAsync(store2, store3);

                res = WaitForValue(() =>
                {
                    using (var session = store3.OpenSession())
                    {
                        return session.Query<User>().Count();
                    }
                }, count, timeout: 60_000, interval: 333);

                Assert.Equal(count, res);

                res = await WaitForValueAsync(async () =>
                {
                    var stats = await GetDatabaseStatisticsAsync(store3);
                    return (int)stats.CountOfRevisionDocuments;
                }, expectedRevisionsCount, timeout: 30_000, interval: 333);

                Assert.Equal(expectedRevisionsCount, res);
            }
        }

        [RavenFact(RavenTestCategory.Counters | RavenTestCategory.Replication)]
        public async Task ReplicationWithCountersToShardedAndThenToNonShardedShouldWork()
        {
            using (var store1 = GetDocumentStore(new Options
            {
                ModifyDatabaseRecord = r =>
                {
                    r.Settings[RavenConfiguration.GetKey(c => c.Replication.MaxItemsCount)] = "1";
                }
            }))
            using (var store2 = Sharding.GetDocumentStore())
            using (var store3 = GetDocumentStore())
            {
                for (int i = 0; i < 100; i++)
                {
                    using (var session = store1.OpenSession())
                    {
                        session.Store(new User { Name = "EGR" }, $"user/322/{i}");
                        session.SaveChanges();

                        var cf = session.CountersFor($"user/322/{i}");
                        cf.Increment("Likes", 100);
                        session.SaveChanges();

                        cf.Increment("Likes2", 200);
                        session.SaveChanges();
                    }
                }

                await SetupReplicationAsync(store1, store2);
                await EnsureReplicatingAsync(store1, store2);

                var replication = await ShardedReplicationTestBase.ShardedReplicationManager.GetShardedReplicationManager(await Sharding.GetShardingConfigurationAsync(store2),
                    new List<RavenServer>() { Server }, store2.Database, new ReplicationManager.ReplicationOptions
                    {
                        BreakReplicationOnStart = true,
                        MaxItemsCount = 1
                    });

                var externalList = await SetupReplicationAsync(store2, store3);
                replication.ShardReplications[2].Mend();
                var id = Sharding.GetRandomIdForShard(await Sharding.GetShardingConfigurationAsync(store2), 2);
                EnsureReplicating(store2, store3, id);

                var external = new ExternalReplication(store2.Database, $"ConnectionString-{store3.Identifier}")
                {
                    TaskId = externalList.First().TaskId,
                    Disabled = true
                };

                await store2.Maintenance.SendAsync(new UpdateExternalReplicationOperation(external));
                external.Disabled = false;
                await store2.Maintenance.SendAsync(new UpdateExternalReplicationOperation(external));

                replication.ShardReplications[0].Mend();
                replication.ShardReplications[1].Mend();

                await EnsureReplicatingAsync(store2, store3);

                var stats = await GetDatabaseStatisticsAsync(store3);
                Assert.Equal(100, stats.CountOfCounterEntries);
            }
        }

        [RavenFact(RavenTestCategory.Replication)]
        public async Task ReplicationToShardedAndThenToNonShardedShouldWork2()
        {
            using (var store1 = GetDocumentStore(new Options
            {
                ModifyDatabaseRecord = r =>
                {
                    r.Settings[RavenConfiguration.GetKey(c => c.Replication.MaxItemsCount)] = "1";
                }
            }))
            using (var store2 = Sharding.GetDocumentStore())
            using (var store3 = GetDocumentStore())
            {
                await store1.Maintenance.SendAsync(new CreateSampleDataOperation(DatabaseItemType.TimeSeries | DatabaseItemType.RevisionDocuments | DatabaseItemType.Documents | 
                                                                                 DatabaseItemType.Attachments | DatabaseItemType.Tombstones | DatabaseItemType.CounterGroups));
              
                await SetupReplicationAsync(store1, store2);
                await EnsureReplicatingAsync(store1, store2);

                var replication = await ShardedReplicationTestBase.ShardedReplicationManager.GetShardedReplicationManager(await Sharding.GetShardingConfigurationAsync(store2),
                    new List<RavenServer>() { Server }, store2.Database, new ReplicationManager.ReplicationOptions
                    {
                        BreakReplicationOnStart = true,
                        MaxItemsCount = 1
                    });

                var externalList = await SetupReplicationAsync(store2, store3);
                replication.ShardReplications[2].Mend();
                var id = Sharding.GetRandomIdForShard(await Sharding.GetShardingConfigurationAsync(store2), 2);
                EnsureReplicating(store2, store3, id);

                var external = new ExternalReplication(store2.Database, $"ConnectionString-{store3.Identifier}")
                {
                    TaskId = externalList.First().TaskId,
                    Disabled = true
                };

                await store2.Maintenance.SendAsync(new UpdateExternalReplicationOperation(external));
                external.Disabled = false;
                await store2.Maintenance.SendAsync(new UpdateExternalReplicationOperation(external));

                replication.ShardReplications[0].Mend();
                replication.ShardReplications[1].Mend();

                await EnsureReplicatingAsync(store2, store3);

                var stats2 = await GetDatabaseStatisticsAsync(store2);
                var stats3 = await GetDatabaseStatisticsAsync(store3);

                Assert.Equal(stats2.CountOfCounterEntries, stats3.CountOfCounterEntries);
                Assert.Equal(stats2.CountOfRevisionDocuments, stats3.CountOfRevisionDocuments);
                Assert.Equal(stats2.CountOfAttachments, stats3.CountOfAttachments);
                Assert.Equal(stats2.CountOfTimeSeriesSegments, stats3.CountOfTimeSeriesSegments);
            }
        }
    }
}
