﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using FastTests.Utils;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes.TimeSeries;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Expiration;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Operations.TimeSeries;
using SlowTests.Core.Utils.Entities;
using Sparrow;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_21681 : ReplicationTestBase
    {
        public RavenDB_21681(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Cluster | RavenTestCategory.Replication | RavenTestCategory.TimeSeries | RavenTestCategory.ExpirationRefresh)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task DeleteExpiredDocumentWithBigTimeSeriesShouldNotCauseReplicationToBreak(Options options)
        {
            var (nodes, leader) = await CreateRaftCluster(2, watcherCluster: true);

            options.Server = leader;
            options.ReplicationFactor = 2;
            using (var store = GetDocumentStore(options))
            {
                var user = new User { Name = "Shiran" };

                var expiry = DateTime.Now.AddYears(-1).ToUniversalTime();
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(user);
                    var metadata = session.Advanced.GetMetadataFor(user);
                    metadata[Constants.Documents.Metadata.Expires] = expiry.ToString(DefaultFormat.DateTimeFormatsToRead[0]);
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var ts = session.TimeSeriesFor(user.Id, "heartbeat");
                    for (int i = 0; i < 500_000; i++)
                        ts.Append(expiry.AddMilliseconds(i), new List<double> { i, i * 100, i * 200, i * int.MaxValue });

                    await session.SaveChangesAsync();
                }

                Assert.True(await WaitForChangeVectorInClusterAsync(nodes, store.Database, 30_000));

                await ExpirationHelper.SetupExpiration(store, leader.ServerStore, new ExpirationConfiguration { Disabled = false, DeleteFrequencyInSec = 5 });

                await WaitAndAssertForValueAsync(async () =>
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        var u = await session.LoadAsync<User>(user.Id);
                        return u == null;
                    }

                }, true);

                var user2 = new User { Name = "Shiran2" };
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(user2);
                    await session.SaveChangesAsync();
                }

                Assert.True(await WaitForDocumentInClusterAsync<User>(nodes, store.Database, user2.Id, u => u.Name == "Shiran2", TimeSpan.FromSeconds(30)));

                foreach (var server in nodes)
                {
                    var stats = await store.Maintenance.SendAsync(new GetStatisticsOperation(server.DebugTag, server.ServerStore.NodeTag));
                    Assert.Equal(0, stats.CountOfTimeSeriesSegments);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.TimeSeries)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public async Task ShouldUpdateMapIndexEntriesAfterDeletingEntireTimeSeries(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                const string id = "users/1";
                var now1 = RavenTestHelper.UtcToday;
                var now2 = now1.AddMinutes(1);

                using (var session = store.OpenSession())
                {
                    var user = new User();
                    session.Store(user, id);
                    session.TimeSeriesFor(user, "HeartRate").Append(now1,1);
                    session.TimeSeriesFor(user, "HeartRate").Append(now2,2);

                    session.SaveChanges();
                }

                var mapIndex = new UsersTimeSeriesMapIndex();
                await mapIndex.ExecuteAsync(store);

                Indexes.WaitForIndexing(store);
                
                Assert.Equal(2, await WaitForValueAsync(() => store.Maintenance.Send(new GetIndexStatisticsOperation(mapIndex.IndexName)).EntriesCount, 2));

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>(id);
                    session.TimeSeriesFor(user, "HeartRate").Delete();

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                Assert.Equal(0, await WaitForValueAsync(() => store.Maintenance.Send(new GetIndexStatisticsOperation(mapIndex.IndexName)).EntriesCount, 0));
            }
        }

        [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.TimeSeries)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public async Task ShouldUpdateMapIndexEntriesAfterDeletingEntireTimeSeriesAfterRetention(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                const string id = "users/1";
                const string tsName = "HeartRate";

                var now1 = RavenTestHelper.UtcToday;
                var now2 = now1.AddMinutes(1);

                using (var session = store.OpenSession())
                {
                    var user = new User();
                    session.Store(user, id);
                    session.TimeSeriesFor(user, tsName).Append(now1, 1);
                    session.TimeSeriesFor(user, tsName).Append(now2, 2);

                    session.SaveChanges();
                }

                var mapIndex = new UsersTimeSeriesMapIndex();
                await mapIndex.ExecuteAsync(store);

                Indexes.WaitForIndexing(store);

                Assert.Equal(2, await WaitForValueAsync(() => store.Maintenance.Send(new GetIndexStatisticsOperation(mapIndex.IndexName)).EntriesCount, 2));

                var config = new TimeSeriesConfiguration
                {
                    Collections = new Dictionary<string, TimeSeriesCollectionConfiguration>
                    {
                        ["Users"] = new TimeSeriesCollectionConfiguration
                        {
                            RawPolicy = new RawTimeSeriesPolicy(TimeSpan.FromHours(1))
                        }
                    }
                };
                await store.Maintenance.SendAsync(new ConfigureTimeSeriesOperation(config));
                
                var database = await Databases.GetDocumentDatabaseInstanceFor(store);
                database.Time.UtcDateTime = () => now1.AddHours(2);

                await database.TimeSeriesPolicyRunner.DoRetention();

                using (var session = store.OpenAsyncSession())
                {
                    var entries = await session.TimeSeriesFor(id, tsName).GetAsync();
                    Assert.Null(entries);
                }

                Indexes.WaitForIndexing(store);

                Assert.Equal(0, await WaitForValueAsync(() => store.Maintenance.Send(new GetIndexStatisticsOperation(mapIndex.IndexName)).EntriesCount, 0));
            }
        }

        [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.TimeSeries)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public async Task ShouldUpdateMapReduceIndexEntriesAfterDeletingEntireTimeSeries(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                const string id = "users/1";
                const string timeSeriesName = "Count";

                await new UsersTimeSeriesMapReduceIndex().ExecuteAsync(store);

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User
                    {
                        Name = "Grisha"
                    }, id);
                    session.TimeSeriesFor(id, timeSeriesName).Append(DateTime.Today, 3);
                    session.TimeSeriesFor(id, timeSeriesName).Append(DateTime.Today.AddMinutes(1), 4);
                    await session.SaveChangesAsync();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenAsyncSession())
                {
                    var usersCount = await session.Query<User, UsersTimeSeriesMapReduceIndex>().CountAsync();
                    Assert.Equal(1, usersCount);
                }

                using (var session = store.OpenAsyncSession())
                {
                    session.Advanced.WaitForIndexesAfterSaveChanges();

                    session.TimeSeriesFor(id, timeSeriesName).Delete();
                    await session.SaveChangesAsync();
                }

                var database = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
                await database.TombstoneCleaner.ExecuteCleanup();

                Indexes.WaitForIndexing(store);
                
                using (var session = store.OpenAsyncSession())
                {
                    var entries = await session.TimeSeriesFor(id, timeSeriesName).GetAsync();
                    Assert.Null(entries);

                    var usersCount = await session.Query<User, UsersTimeSeriesMapReduceIndex>().CountAsync();
                    Assert.Equal(0, usersCount);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.TimeSeries)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public async Task ShouldUpdateMapReduceIndexEntriesAfterDeletingEntireTimeSeriesAfterRetention(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                const string id = "users/1";
                const string tsName = "HeartRate";

                var now1 = RavenTestHelper.UtcToday;
                var now2 = now1.AddMinutes(1);

                await new UsersTimeSeriesMapReduceIndex().ExecuteAsync(store);

                using (var session = store.OpenSession())
                {
                    var user = new User();
                    session.Store(user, id);
                    session.TimeSeriesFor(user, tsName).Append(now1, 1);
                    session.TimeSeriesFor(user, tsName).Append(now2, 2);

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenAsyncSession())
                {
                    var usersCount = await session.Query<User, UsersTimeSeriesMapReduceIndex>().CountAsync();
                    Assert.Equal(1, usersCount);
                }

                var config = new TimeSeriesConfiguration
                {
                    Collections = new Dictionary<string, TimeSeriesCollectionConfiguration>
                    {
                        ["Users"] = new TimeSeriesCollectionConfiguration
                        {
                            RawPolicy = new RawTimeSeriesPolicy(TimeSpan.FromHours(1))
                        }
                    }
                };
                await store.Maintenance.SendAsync(new ConfigureTimeSeriesOperation(config));

                var database = await Databases.GetDocumentDatabaseInstanceFor(store);
                database.Time.UtcDateTime = () => now1.AddHours(2);

                await database.TimeSeriesPolicyRunner.DoRetention();

                using (var session = store.OpenAsyncSession())
                {
                    var entries = await session.TimeSeriesFor(id, tsName).GetAsync();
                    Assert.Null(entries);
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenAsyncSession())
                {
                    var entries = await session.TimeSeriesFor(id, tsName).GetAsync();
                    Assert.Null(entries);

                    var usersCount = await session.Query<User, UsersTimeSeriesMapReduceIndex>().CountAsync();
                    Assert.Equal(0, usersCount);
                }
            }
        }

        private class UsersTimeSeriesMapIndex : AbstractTimeSeriesIndexCreationTask<User>
        {
            public UsersTimeSeriesMapIndex()
            {
                AddMap(
                    "HeartRate",
                    timeSeries => from ts in timeSeries
                        from entry in ts.Entries
                        select new
                        {
                            HeartBeat = entry.Values[0],
                            ts.Name,
                            entry.Timestamp.Date,
                            User = ts.DocumentId
                        });
            }
        }

        private class UsersTimeSeriesMapReduceIndex : AbstractTimeSeriesIndexCreationTask<User>
        {
            public UsersTimeSeriesMapReduceIndex()
            {
                AddMapForAll(timeSeries =>
                    from ts in timeSeries
                    from entry in ts.Entries
                    select new
                    {
                        Name = ts.Name,
                        Count = 1
                    });

                Reduce = results => from result in results
                    group result by new { result.Name } into g
                    select new
                    {
                        Name = g.Key.Name,
                        Count = g.Sum(x => x.Count)
                    };
            }
        }
    }
}
