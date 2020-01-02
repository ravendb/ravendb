using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
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
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    session.Store(new User(), "users/ayende");

                    var tsf = session.TimeSeriesFor("users/ayende");

                    tsf.Append("Heartrate", baseline.AddMinutes(1), "fitbit", new[] { 58d });
                    tsf.Append("Heartrate", baseline.AddMinutes(5), "fitbit", new[] { 68d });
                    tsf.Append("Heartrate", baseline.AddMinutes(10), "fitbit", new[] { 78d });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var all = session.TimeSeriesFor("users/ayende").Get("Heartrate", DateTime.MinValue, DateTime.MaxValue).ToList();
                    Assert.Equal(3, all.Count);
                }

                using (var session = store.OpenSession())
                {
                    session.TimeSeriesFor("users/ayende")
                        .Remove("Heartrate", baseline.AddMinutes(2), baseline.AddMinutes(8));

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var all = session.TimeSeriesFor("users/ayende").Get("Heartrate", DateTime.MinValue, DateTime.MaxValue).ToList();
                    Assert.Equal(2, all.Count);
                }

                using (var session = store.OpenSession())
                {
                    var tsf = session.TimeSeriesFor("users/ayende");

                    tsf.Append("Heartrate", baseline.AddMinutes(5), "fitbit", new[] { 99d });
                    session.SaveChanges();
                }


                using (var session = store.OpenSession())
                {
                    var all = session.TimeSeriesFor("users/ayende").Get("Heartrate", DateTime.MinValue, DateTime.MaxValue).ToList();
                    
                    Assert.Equal(3, all.Count);
                    Assert.Equal(all[1].Values, new []{ 99d });
                }

            }
        }

        [Fact (Skip = "RavenDB-14426")]
        public async Task MergeReInsertedTimeSeriesEntryOnConflict()
        {
            using (var storeA = GetDocumentStore())
            using (var storeB = GetDocumentStore())
            {
                var baseline = DateTime.Now;
                using (var session = storeA.OpenAsyncSession())
                {
                    await session.StoreAsync(new Core.Utils.Entities.User { Name = "Karmel" }, "users/1");

                    var tsf = session.TimeSeriesFor("users/1");

                    tsf.Append("heartbeat", baseline, "herz", new List<double> { 10 });
                    tsf.Append("heartbeat", baseline.AddMinutes(10), "herz", new List<double> { 20 });

                    await session.SaveChangesAsync();
                }


                using (var session = storeA.OpenAsyncSession())
                {
                    // remove an entry

                    session.TimeSeriesFor("users/1").Remove("heartbeat", baseline.AddMinutes(5), baseline.AddMinutes(15));

                    await session.SaveChangesAsync();
                }

                await SetupReplicationAsync(storeA, storeB);
                EnsureReplicating(storeA, storeB);

                foreach (var store in new[] { storeA, storeB })
                {
                    // verify that the entry is removed 

                    using (var session = store.OpenAsyncSession())
                    {
                        var all = (await session.TimeSeriesFor("users/1").GetAsync("heartbeat", DateTime.MinValue, DateTime.MaxValue)).ToList();

                        Assert.Equal(1, all.Count);
                    }

                }

                using (var session = storeB.OpenAsyncSession())
                {
                    // create a conflict on Segment's Change Vector,
                    // so that incoming replication (from A to B) won't be able to append entire segment 

                    session.TimeSeriesFor("users/1").Append("heartbeat", baseline.AddMinutes(7.5), "herz", new List<double> { 30 });

                    await session.SaveChangesAsync();

                    var all = (await session.TimeSeriesFor("users/1").GetAsync("heartbeat", DateTime.MinValue, DateTime.MaxValue)).ToList();

                    Assert.Equal(2, all.Count);
                }


                using (var session = storeA.OpenAsyncSession())
                {
                    // re insert the deleted entry, with new values 

                    session.TimeSeriesFor("users/1").Append("heartbeat", baseline.AddMinutes(10), "herz2", new List<double> { 5 }); // works fine if new value is greater than old value

                    await session.SaveChangesAsync();

                    // verify that the series contains the re-inserted entry

                    var all = await session.TimeSeriesFor("users/1").GetAsync("heartbeat", DateTime.MinValue, DateTime.MaxValue);

                    var values = all.Select(x => x.Value).ToList();

                    Assert.Equal(2, all.Count());
                    Assert.Contains(5d, values);
                }


                EnsureReplicating(storeA, storeB);

                using (var session = storeB.OpenAsyncSession())
                {
                    // the re-inserted entry should be replicated to B

                    var all = (await session.TimeSeriesFor("users/1").GetAsync("heartbeat", DateTime.MinValue, DateTime.MaxValue)).ToList();

                    Assert.Equal(3, all.Count); // fails here
                }
            }
        }
    }
}
