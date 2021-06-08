using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using FastTests.Server.Replication;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client.TimeSeries.Issues
{
    public class RavenDB_14426 : ReplicationTestBase
    {
        public RavenDB_14426(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanDeleteAndReInsertTimeSeriesEntry()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    session.Store(new User(), "users/ayende");

                    var tsf = session.TimeSeriesFor("users/ayende", "Heartrate");

                    tsf.Append(baseline.AddMinutes(1), new[] { 58d }, "fitbit");
                    tsf.Append(baseline.AddMinutes(5), new[] { 68d }, "fitbit");
                    tsf.Append( baseline.AddMinutes(10), new[] { 78d }, "fitbit");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var all = session.TimeSeriesFor("users/ayende", "Heartrate").Get(DateTime.MinValue, DateTime.MaxValue).ToList();
                    Assert.Equal(3, all.Count);
                }

                using (var session = store.OpenSession())
                {
                    session.TimeSeriesFor("users/ayende", "Heartrate")
                        .Delete(baseline.AddMinutes(2), baseline.AddMinutes(8));

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var all = session.TimeSeriesFor("users/ayende", "Heartrate").Get(DateTime.MinValue, DateTime.MaxValue).ToList();
                    Assert.Equal(2, all.Count);
                }

                using (var session = store.OpenSession())
                {
                    var tsf = session.TimeSeriesFor("users/ayende", "Heartrate");

                    tsf.Append(baseline.AddMinutes(5), new[] { 99d }, "fitbit");
                    session.SaveChanges();
                }


                using (var session = store.OpenSession())
                {
                    var all = session.TimeSeriesFor("users/ayende", "Heartrate").Get(DateTime.MinValue, DateTime.MaxValue).ToList();
                    
                    Assert.Equal(3, all.Count);
                    Assert.Equal(all[1].Values, new []{ 99d });
                }

            }
        }

        [Fact]
        public async Task MergeReInsertedTimeSeriesEntryOnConflict()
        {
            using (var storeA = GetDocumentStore())
            using (var storeB = GetDocumentStore())
            {
                var baseline = DateTime.Now;
                using (var session = storeA.OpenAsyncSession())
                {
                    await session.StoreAsync(new Core.Utils.Entities.User { Name = "Karmel" }, "users/1");

                    var tsf = session.TimeSeriesFor("users/1", "heartbeat");

                    tsf.Append(baseline, new List<double> { 10 }, "herz");
                    tsf.Append(baseline.AddMinutes(10), new List<double> { 20 }, "herz");

                    await session.SaveChangesAsync();
                }

                using (var session = storeA.OpenAsyncSession())
                {
                    // remove an entry

                    session.TimeSeriesFor("users/1", "heartbeat").Delete(baseline.AddMinutes(5), baseline.AddMinutes(15));

                    await session.SaveChangesAsync();
                }

                await SetupReplicationAsync(storeA, storeB);
                EnsureReplicating(storeA, storeB);

                foreach (var store in new[] { storeA, storeB })
                {
                    // verify that the entry is removed 

                    using (var session = store.OpenAsyncSession())
                    {
                        var all = (await session.TimeSeriesFor("users/1", "heartbeat").GetAsync(DateTime.MinValue, DateTime.MaxValue)).ToList();

                        Assert.Equal(1, all.Count);
                    }

                }

                using (var session = storeB.OpenAsyncSession())
                {
                    // create a conflict on Segment's Change Vector,
                    // so that incoming replication (from A to B) won't be able to append entire segment 

                    session.TimeSeriesFor("users/1", "heartbeat").Append(baseline.AddMinutes(7.5), new List<double> { 30 }, "herz");

                    await session.SaveChangesAsync();

                    var all = (await session.TimeSeriesFor("users/1", "heartbeat").GetAsync(DateTime.MinValue, DateTime.MaxValue)).ToList();

                    Assert.Equal(2, all.Count);
                }


                using (var session = storeA.OpenAsyncSession())
                {
                    // re insert the deleted entry, with new values 

                    session.TimeSeriesFor("users/1", "heartbeat").Append(baseline.AddMinutes(10), new List<double> { 5 }, "herz2"); // works fine if new value is greater than old value

                    await session.SaveChangesAsync();

                    // verify that the series contains the re-inserted entry

                    var all = await session.TimeSeriesFor("users/1", "heartbeat").GetAsync(DateTime.MinValue, DateTime.MaxValue);

                    var values = all.Select(x => x.Value).ToList();

                    Assert.Equal(2, all.Count());
                    Assert.Contains(5d, values);
                }


                EnsureReplicating(storeA, storeB);

                await SetupReplicationAsync(storeB, storeA);
                EnsureReplicating(storeB, storeA);

                using (var sessionB = storeB.OpenAsyncSession())
                using (var sessionA = storeA.OpenAsyncSession())
                {
                    var allFromA = (await sessionA.TimeSeriesFor("users/1", "heartbeat").GetAsync(DateTime.MinValue, DateTime.MaxValue)).ToList();
                    var allFromB = (await sessionB.TimeSeriesFor("users/1", "heartbeat").GetAsync(DateTime.MinValue, DateTime.MaxValue)).ToList();

                    Assert.Equal(allFromB.Count, allFromA.Count);

                    for (int i = 0; i < allFromA.Count; i++)
                    {
                        var fromA = allFromA[i];
                        var fromB = allFromB[i];

                        Assert.Equal(fromA.Tag,fromB.Tag);
                        Assert.Equal(fromA.Timestamp, fromB.Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                        Assert.Equal(fromA.Value,fromB.Value);
                        Assert.Equal(fromA.Values,fromB.Values);
                    }
                }
            }
        }
    }
}
