using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;
using System;
using FastTests.Server.Replication;
using SlowTests.Core.Utils.Entities;

namespace SlowTests.Issues
{
    public class RavenDB_19699 : ReplicationTestBase
    {
        
        public RavenDB_19699(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task ShouldCreateAndReplicateTimeSeriesDeletedRangeWhenDeletingDocument()
        {
            const string id = "users/1";
            using (var store = GetDocumentStore())
            using (var replica = GetDocumentStore())
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

                var brokenReplication = await BreakReplication(Server.ServerStore, store.Database);

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

                brokenReplication.Mend();

                Assert.True(WaitForDocument<User>(replica, id, u => u.Name == "ayende2"));

                using (var session = replica.OpenAsyncSession())
                {
                    var tsEntries = await session.TimeSeriesFor(id, "heartrate").GetAsync();
                    Assert.Equal(1, tsEntries.Length);
                }

            }
        }

        [Fact]
        public async Task DeletingTimeSeriesAndModifyingItsDocumentShouldNotResultInConflictOnReplica()
        {
            const string id = "users/1";
            using (var store = GetDocumentStore())
            using (var replica = GetDocumentStore())
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
    }
}
