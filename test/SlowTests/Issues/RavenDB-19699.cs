using System;
using System.Threading.Tasks;
using SlowTests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_19699 : ReplicationTestBase
    {
        public RavenDB_19699(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.TimeSeries | RavenTestCategory.Replication)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task ShouldCreateAndReplicateTimeSeriesDeletedRangeWhenDeletingDocument(Options options)
        {
            const string id = "users/1";
            using (var store = GetDocumentStore(options))
            using (var replica = GetDocumentStore(options))
            {
                var baseline = DateTime.Now;

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User
                    {
                        Name = "ayende"
                    }, id);

                    var tsf = session.TimeSeriesFor(id, "heartrate");
                    tsf.Append(baseline, 1);
                    tsf.Append(baseline.AddDays(1), 1);
                    tsf.Append(baseline.AddDays(2), 1);

                    await session.SaveChangesAsync();
                }

                await SetupReplicationAsync(store, replica);

                Assert.True(WaitForDocument<User>(replica, id, u => u.Name == "ayende"));

                using (var session = replica.OpenAsyncSession())
                {
                    var tsEntries = await session.TimeSeriesFor(id, "heartrate").GetAsync();
                    Assert.Equal(3, tsEntries.Length);
                }

                using var replication = await GetReplicationManagerAsync(store, store.Database, options.DatabaseMode, breakReplication: true);

                using (var session = store.OpenAsyncSession())
                {
                    session.Delete(id);
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User
                    {
                        Name = "ayende2"
                    }, id);

                    session.TimeSeriesFor(id, "heartrate").Append(baseline.AddDays(3), 1);

                    await session.SaveChangesAsync();
                }

                replication.Mend();

                Assert.True(WaitForDocument<User>(replica, id, u => u.Name == "ayende2"));

                using (var session = replica.OpenAsyncSession())
                {
                    var tsEntries = await session.TimeSeriesFor(id, "heartrate").GetAsync();
                    Assert.Equal(1, tsEntries.Length);
                }
            }
        }

        [RavenTheory(RavenTestCategory.TimeSeries | RavenTestCategory.Replication)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task DeletingTimeSeriesAndModifyingItsDocumentShouldNotResultInConflictOnReplica(Options options)
        {
            const string id = "users/1";
            using (var store = GetDocumentStore(options))
            using (var replica = GetDocumentStore(options))
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User
                    {
                        Name = "ayende"
                    }, id);

                    session.TimeSeriesFor(id, "heartrate").Append(DateTime.Now, 1);

                    await session.SaveChangesAsync();
                }

                await SetupReplicationAsync(store, replica);

                Assert.True(WaitForDocument<User>(replica, id, u => u.Name == "ayende"));

                using (var session = replica.OpenAsyncSession())
                {
                    var tsEntries = await session.TimeSeriesFor(id, "heartrate").GetAsync();
                    Assert.Equal(1, tsEntries.Length);
                }

                using (var session = store.OpenAsyncSession())
                {
                    // delete timeseries and update document
                    session.TimeSeriesFor(id, "heartrate").Delete();

                    var doc = await session.LoadAsync<User>(id);
                    doc.Name = "ayende2";

                    await session.SaveChangesAsync();
                }

                Assert.True(WaitForDocument<User>(replica, id, u => u.Name == "ayende2"));

                using (var session = replica.OpenAsyncSession())
                {
                    var ts = await session.TimeSeriesFor(id, "heartrate").GetAsync();
                    Assert.Null(ts);
                }
            }
        }

        [RavenTheory(RavenTestCategory.TimeSeries | RavenTestCategory.Replication)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task ConflictBetweenDocWithTsAndDeletion_WhenDeletionWins(Options options)
        {
            const string id = "users/1";
            using (var store = GetDocumentStore(options))
            using (var replica = GetDocumentStore(options))
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User
                    {
                        Name = "ayende"
                    }, id);

                    await session.SaveChangesAsync();
                }

                await SetupReplicationAsync(store, replica);

                Assert.True(WaitForDocument<User>(replica, id, u => u.Name == "ayende"));

                using var replication = await GetReplicationManagerAsync(store, store.Database, options.DatabaseMode, breakReplication: true);

                using (var session = replica.OpenAsyncSession())
                {
                    session.TimeSeriesFor(id, "heartrate").Append(DateTime.UtcNow, 1);
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    session.Delete(id);
                    await session.SaveChangesAsync();
                }

                replication.Mend();

                Assert.True(WaitForDocumentDeletion(replica, id, 5000));

                using (var session = replica.OpenAsyncSession())
                {
                    var ts = await session.TimeSeriesFor(id, "heartrate").GetAsync();
                    Assert.Null(ts);
                }
            }
        }

        [RavenTheory(RavenTestCategory.TimeSeries | RavenTestCategory.Replication)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task ConflictBetweenDocWithAndDocWithoutTs_WhenDocWithoutTsWins(Options options)
        {
            const string id = "users/1";
            using (var store = GetDocumentStore(options))
            using (var replica = GetDocumentStore(options))
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User
                    {
                        Name = "ayende"
                    }, id);

                    await session.SaveChangesAsync();
                }

                using var replication = await GetReplicationManagerAsync(store, store.Database, options.DatabaseMode);
                await SetupReplicationAsync(store, replica);

                Assert.True(WaitForDocument<User>(replica, id, u => u.Name == "ayende"));

                replication.Break();

                using (var session = store.OpenAsyncSession())
                {
                    session.TimeSeriesFor(id, "heartrate").Append(DateTime.UtcNow, 1);
                    await session.SaveChangesAsync();
                }

                using (var session = replica.OpenAsyncSession())
                {
                    var doc = await session.LoadAsync<User>(id);
                    doc.Name = "ayende2";

                    await session.SaveChangesAsync();
                }

                replication.Mend();

                await AssertWaitForNotNullAsync(async () =>
                {
                    using var session = replica.OpenAsyncSession();
                    return await session.TimeSeriesFor(id, "heartrate").GetAsync();
                });

                using (var session = replica.OpenAsyncSession())
                {
                    var doc = await session.LoadAsync<User>(id);
                    Assert.Equal("ayende2", doc.Name);
                }
            }
        }
    }
}
