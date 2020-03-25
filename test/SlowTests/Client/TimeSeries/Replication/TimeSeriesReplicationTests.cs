using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Session;
using Raven.Server.Documents;
using SlowTests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client.TimeSeries.Replication
{
    public class TimeSeriesReplicationTests : ReplicationTestBase
    {
        public TimeSeriesReplicationTests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task CanReplicate()
        {
            using (var storeA = GetDocumentStore())
            using (var storeB = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = storeA.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, "users/ayende");
                    session.TimeSeriesFor("users/ayende", "Heartrate")
                        .Append(baseline.AddMinutes(1), new[] { 59d }, "watches/fitbit");
                    session.SaveChanges();
                }

                await SetupReplicationAsync(storeA, storeB);

                WaitForUserToContinueTheTest(storeA);

                EnsureReplicating(storeA, storeB);

                using (var session = storeB.OpenSession())
                {
                    var val = session.TimeSeriesFor("users/ayende", "Heartrate")
                        .Get(DateTime.MinValue, DateTime.MaxValue)
                        .Single();
                    Assert.Equal(new[] { 59d }, val.Values);
                    Assert.Equal("watches/fitbit", val.Tag);
                    Assert.Equal(baseline.AddMinutes(1), val.Timestamp);
                }
            }
        }

        [Fact]
        public async Task CanReplicateValuesOutOfOrder()
        {
            using (var storeA = GetDocumentStore())
            using (var storeB = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = storeA.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, "users/ayende");
                    session.SaveChanges();
                }

                using (var session = storeB.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, "users/ayende");
                    session.SaveChanges();
                }

                const int retries = 1000;

                var offset = 0;

                using (var session = storeA.OpenSession())
                {

                    for (int j = 0; j < retries; j++)
                    {
                        session.TimeSeriesFor("users/ayende", "Heartrate")
                            .Append(baseline.AddMinutes(offset), new double[] { offset }, "watches/fitbit");

                        offset += 5;
                    }

                    session.SaveChanges();
                }

                offset = 1;

                using (var session = storeB.OpenSession())
                {

                    for (int j = 0; j < retries; j++)
                    {
                        session.TimeSeriesFor("users/ayende", "Heartrate")
                            .Append(baseline.AddMinutes(offset), new double[] { offset }, "watches/fitbit");
                        offset += 5;
                    }

                    session.SaveChanges();
                }

                await SetupReplicationAsync(storeA, storeB);
                await SetupReplicationAsync(storeB, storeA);

                WaitForUserToContinueTheTest(storeA);

                EnsureReplicating(storeA, storeB);
                EnsureReplicating(storeB, storeA);

                using (var session = storeB.OpenSession())
                {
                    var vals = session.TimeSeriesFor("users/ayende", "Heartrate")
                        .Get(DateTime.MinValue, DateTime.MaxValue)
                        .ToList();
                    Assert.Equal(2 * retries, vals.Count);

                    offset = 0;
                    for (int i = 0; i < retries; i++)
                    {
                        Assert.Equal(baseline.AddMinutes(offset), vals[i].Timestamp);
                        Assert.Equal(offset, vals[i].Values[0]);

                        offset++;
                        i++;

                        Assert.Equal(baseline.AddMinutes(offset), vals[i].Timestamp);
                        Assert.Equal(offset, vals[i].Values[0]);


                        offset += 4;
                    }
                }

                using (var session = storeA.OpenSession())
                {
                    var vals = session.TimeSeriesFor("users/ayende", "Heartrate")
                        .Get(DateTime.MinValue, DateTime.MaxValue)
                        .ToList();
                    Assert.Equal(2 * retries, vals.Count);

                    offset = 0;
                    for (int i = 0; i < retries; i++)
                    {
                        Assert.Equal(baseline.AddMinutes(offset), vals[i].Timestamp);
                        Assert.Equal(offset, vals[i].Values[0]);

                        offset++;
                        i++;

                        Assert.Equal(baseline.AddMinutes(offset), vals[i].Timestamp);
                        Assert.Equal(offset, vals[i].Values[0]);


                        offset += 4;
                    }
                }
            }
        }

        [Fact]
        public async Task CanReplicateFullSegment()
        {
            using (var storeA = GetDocumentStore())
            using (var storeB = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                // this is not thread-safe intentionally, to have duplicate values
                var mainRand = new Random(357);
                var num = 0;


                /*var t1 = Task.Run(() => Insert(storeA, mainRand.Next(0, 1000)));
                var t2 = Task.Run(() => Insert(storeB, mainRand.Next(0, 1000)));*/
                Insert(storeA, mainRand.Next(0, 1000));
                Insert(storeB, mainRand.Next(0, 1000));

                //await Task.WhenAll(t1, t2);
                void Insert(IDocumentStore store, int seed)
                {
                    var offset = 0;
                    var value = Interlocked.Increment(ref num) * 1000;

                    var rand = new Random(seed);
                    using (var session = store.OpenSession())
                    {
                        session.Store(new { Name = "Oren" }, "users/ayende");
                        session.SaveChanges();
                    }

                    for (int i = 0; i < 10; i++)
                    {
                        using (var session = store.OpenSession())
                        {
                            for (int j = 0; j < 100; j++)
                            {
                                session.TimeSeriesFor("users/ayende", "Heartrate")
                                    .Append(baseline.AddMinutes(offset), new double[] { value++ }, "watches/fitbit");
                                offset += rand.Next(1, 5);
                            }

                            session.SaveChanges();
                        }
                    }
                }

                await SetupReplicationAsync(storeA, storeB);
                EnsureReplicating(storeA, storeB);

                using (var sessionB = storeB.OpenSession())
                {
                    var valsB = sessionB.TimeSeriesFor("users/ayende", "Heartrate")
                        .Get(DateTime.MinValue, DateTime.MaxValue)
                        .ToList();

                    Assert.Equal(valsB.Select(x => x.Timestamp).Distinct().Count(), valsB.Count);

                    Assert.True(valsB.SequenceEqual(valsB.OrderBy(x => x.Timestamp)));
                }


                await SetupReplicationAsync(storeB, storeA);
                EnsureReplicating(storeB, storeA);

                await Task.Delay(3000);

                using (var sessionA = storeA.OpenSession())
                using (var sessionB = storeB.OpenSession())
                {
                    var valsA = sessionA.TimeSeriesFor("users/ayende", "Heartrate")
                        .Get(DateTime.MinValue, DateTime.MaxValue)
                        .ToList();

                    Assert.True(valsA.SequenceEqual(valsA.OrderBy(x => x.Timestamp)));

                    var valsB = sessionB.TimeSeriesFor("users/ayende", "Heartrate")
                        .Get(DateTime.MinValue, DateTime.MaxValue)
                        .ToList();

                    Assert.True(valsB.SequenceEqual(valsB.OrderBy(x => x.Timestamp)));

                    Assert.Equal(valsB.Count, valsA.Count);

                    for (int i = 0; i < valsA.Count; i++)
                    {
                        Assert.Equal(valsB[i].Tag, valsA[i].Tag);
                        Assert.Equal(valsA[i].Timestamp, valsB[i].Timestamp);

                        Assert.Equal(valsB[i].Values.Length, valsA[i].Values.Length);
                        for (int j = 0; j < valsA[i].Values.Length; j++)
                        {
                            Assert.True(valsB[i].Values[j].Equals(valsA[i].Values[j]),$"Values aren't equal at {valsA[i].Timestamp} ({valsA[i].Timestamp.Ticks})");
                        }
                    }
                }
            }
        }

        [Fact]
        public async Task CanReplicateMany()
        {
            using (var storeA = GetDocumentStore())
            using (var storeB = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                // this is not thread-safe intentionally, to have duplicate values
                var offset = 0;
                var value = 0;

                var t1 = Task.Run(() => Insert(storeA));
                var t2 = Task.Run(() => Insert(storeB));

                await Task.WhenAll(t1, t2);
                void Insert(IDocumentStore store)
                {
                    using (var session = store.OpenSession())
                    {
                        session.Store(new { Name = "Oren" }, "users/ayende");
                        session.SaveChanges();
                    }

                    for (int i = 0; i < 100; i++)
                    {
                        using (var session = store.OpenSession())
                        {
                            for (int j = 0; j < 100; j++)
                            {
                                session.TimeSeriesFor("users/ayende", "Heartrate")
                                    .Append(baseline.AddMinutes(offset++), new double[] { value++ }, "watches/fitbit");
                            }

                            session.SaveChanges();
                        }
                    }
                }

                await SetupReplicationAsync(storeA, storeB);
                await SetupReplicationAsync(storeB, storeA);

                EnsureReplicating(storeA, storeB);
                EnsureReplicating(storeB, storeA);

                using (var sessionA = storeA.OpenSession())
                using (var sessionB = storeB.OpenSession())
                {
                    var valsA = sessionA.TimeSeriesFor("users/ayende", "Heartrate")
                        .Get(DateTime.MinValue, DateTime.MaxValue)
                        .ToList();

                    var valsB = sessionB.TimeSeriesFor("users/ayende", "Heartrate")
                        .Get(DateTime.MinValue, DateTime.MaxValue)
                        .ToList();

                    Assert.Equal(valsB.Count, valsA.Count);

                    for (int i = 0; i < valsA.Count; i++)
                    {
                        Assert.Equal(valsB[i].Tag, valsA[i].Tag);
                        Assert.Equal(valsB[i].Timestamp, valsA[i].Timestamp);
                        
                        Assert.Equal(valsB[i].Values.Length, valsA[i].Values.Length);
                        for (int j = 0; j < valsA[i].Values.Length; j++)
                        {
                            Assert.Equal(valsB[i].Values[j], valsA[i].Values[j]);
                        }
                    }
                }
            }
        }

        [Fact]
        public async Task CanReplicateManyWithDeletions()
        {
            using (var storeA = GetDocumentStore())
            using (var storeB = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                // this is not thread-safe intentionally, to have duplicate values
                var offset = 0;
                var value = 0;

                var t1 = Task.Run(() => Insert(storeA));
                var t2 = Task.Run(() => Insert(storeB));

                await Task.WhenAll(t1, t2);
                void Insert(IDocumentStore store)
                {
                    using (var session = store.OpenSession())
                    {
                        session.Store(new { Name = "Oren" }, "users/ayende");
                        session.SaveChanges();
                    }

                    for (int i = 0; i < 100; i++)
                    {
                        using (var session = store.OpenSession())
                        {
                            var tsf = session.TimeSeriesFor("users/ayende", "Heartrate");

                            for (int j = 0; j < 100; j++)
                            {
                                var time = baseline.AddMinutes(offset++);
                                var val = value++;

                                tsf.Append(time, new double[] { val }, "watches/fitbit");
                            }

                            tsf.Remove(baseline.AddMinutes(i + 0.5), baseline.AddMinutes(i + 10.5));

                            session.SaveChanges();
                        }
                    }
                }

                await SetupReplicationAsync(storeA, storeB);
                await SetupReplicationAsync(storeB, storeA);

                await Task.Delay(3000); // wait for replication ping-pong to settle down

                EnsureReplicating(storeA, storeB);
                EnsureReplicating(storeB, storeA);

                using (var sessionA = storeA.OpenSession())
                using (var sessionB = storeB.OpenSession())
                {
                    var valsA = sessionA.TimeSeriesFor("users/ayende", "Heartrate")
                        .Get(DateTime.MinValue, DateTime.MaxValue)
                        .ToList();

                    var valsB = sessionB.TimeSeriesFor("users/ayende", "Heartrate")
                        .Get(DateTime.MinValue, DateTime.MaxValue)
                        .ToList();

                    Assert.Equal(valsB.Count, valsA.Count);

                    for (int i = 0; i < valsA.Count; i++)
                    {
                        Assert.Equal(valsB[i].Tag, valsA[i].Tag);
                        Assert.Equal(valsB[i].Timestamp, valsA[i].Timestamp);

                        Assert.Equal(valsB[i].Values.Length, valsA[i].Values.Length);
                        for (int j = 0; j < valsA[i].Values.Length; j++)
                        {
                            Assert.Equal(valsB[i].Values[j], valsA[i].Values[j]);
                        }
                    }
                }
            }
        }

        [Fact]
        public async Task CanReplicateDeletions()
        {
            using (var storeA = GetDocumentStore())
            using (var storeB = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = storeA.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, "users/ayende");
                    session.TimeSeriesFor("users/ayende", "Heartrate")
                        .Append(baseline.AddMinutes(10), new double[] { 1 }, "watches/fitbit");
                    session.TimeSeriesFor("users/ayende", "Heartrate")
                        .Append(baseline.AddMinutes(1), new double[] { 1 }, "watches/fitbit");
                    session.SaveChanges();
                }

                using (var session = storeB.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, "users/ayende");
                    session.TimeSeriesFor("users/ayende", "Heartrate")
                        .Append(baseline.AddMinutes(2), new double[] { 1 }, "watches/fitbit");
                    session.TimeSeriesFor("users/ayende", "Heartrate")
                        .Append(baseline.AddMinutes(3), new double[] { 1 }, "watches/fitbit");
                    session.SaveChanges();
                }

                await SetupReplicationAsync(storeB, storeA);
                EnsureReplicating(storeB, storeA);

                await SetupReplicationAsync(storeA, storeB);
                EnsureReplicating(storeA, storeB);

                var stats1 = await storeA.Maintenance.ForDatabase(storeA.Database).SendAsync(new GetStatisticsOperation("test"));
                var stats2 = await storeB.Maintenance.ForDatabase(storeB.Database).SendAsync(new GetStatisticsOperation("test"));

                Assert.Equal(1, stats1.CountOfTimeSeriesSegments);
                Assert.Equal(2, stats2.CountOfTimeSeriesSegments);
                
                using (var session = storeA.OpenSession())
                {
                    session.TimeSeriesFor("users/ayende", "Heartrate")
                        .Remove(baseline.AddMinutes(10));
                    session.SaveChanges();
                }

                WaitForUserToContinueTheTest(storeA);

                EnsureReplicating(storeA, storeB);

                using (var sessionA = storeA.OpenSession())
                using (var sessionB = storeB.OpenSession())
                {
                    var valsA = sessionA.TimeSeriesFor("users/ayende", "Heartrate")
                        .Get(DateTime.MinValue, DateTime.MaxValue)
                        .ToList();

                    var valsB = sessionB.TimeSeriesFor("users/ayende", "Heartrate")
                        .Get(DateTime.MinValue, DateTime.MaxValue)
                        .ToList();

                    Assert.Equal(valsA.Count, valsB.Count);

                    for (int i = 0; i < valsA.Count; i++)
                    {
                        Assert.Equal(valsB[i].Tag, valsA[i].Tag);
                        Assert.Equal(valsB[i].Timestamp, valsA[i].Timestamp);

                        Assert.Equal(valsB[i].Values.Length, valsA[i].Values.Length);
                        for (int j = 0; j < valsA[i].Values.Length; j++)
                        {
                            Assert.Equal(valsB[i].Values[j], valsA[i].Values[j]);
                        }
                    }
                }
            }
        }

        [Fact]
        public async Task MergeValues()
        {
            using (var storeA = GetDocumentStore())
            using (var storeB = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = storeA.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, "users/ayende");
                    session.TimeSeriesFor("users/ayende", "Heartrate")
                        .Append(baseline.AddMinutes(1), new[] { 59d }, "watches/fitbit");
                    session.SaveChanges();
                }

                using (var session = storeB.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, "users/ayende");
                    session.TimeSeriesFor("users/ayende", "Heartrate")
                        .Append(baseline.AddMinutes(2), new[] { 70d }, "watches/fitbit2");
                    session.SaveChanges();
                }

                await SetupReplicationAsync(storeA, storeB);
                EnsureReplicating(storeA, storeB);

                await SetupReplicationAsync(storeB, storeA);
                EnsureReplicating(storeB, storeA);

                AssertValues(storeA);
                AssertValues(storeB);

                void AssertValues(IDocumentStore store)
                {
                    using (var session = store.OpenSession())
                    {
                        var values = session.TimeSeriesFor("users/ayende", "Heartrate")
                            .Get(DateTime.MinValue, DateTime.MaxValue).ToList();

                        var val = values[0];
                        Assert.Equal(new[] { 59d }, val.Values);
                        Assert.Equal("watches/fitbit", val.Tag);
                        Assert.Equal(baseline.AddMinutes(1), val.Timestamp);

                        val = values[1];
                        Assert.Equal(new[] { 70d }, val.Values);
                        Assert.Equal("watches/fitbit2", val.Tag);
                        Assert.Equal(baseline.AddMinutes(2), val.Timestamp);
                    }
                }
            }
        }

        [Fact]
        public async Task HigherValueWins()
        {
            using (var storeA = GetDocumentStore())
            using (var storeB = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = storeA.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, "users/ayende");
                    session.TimeSeriesFor("users/ayende", "Heartrate")
                        .Append(baseline.AddMinutes(1), new[] { 59d }, "watches/fitbit");
                    session.SaveChanges();
                }

                using (var session = storeB.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, "users/ayende");
                    session.TimeSeriesFor("users/ayende", "Heartrate")
                        .Append(baseline.AddMinutes(1), new[] { 70d }, "watches/fitbit");
                    session.SaveChanges();
                }

                await SetupReplicationAsync(storeA, storeB);
                EnsureReplicating(storeA, storeB);

                await SetupReplicationAsync(storeB, storeA);
                EnsureReplicating(storeB, storeA);

                AssertValues(storeA);
                AssertValues(storeB);

                void AssertValues(IDocumentStore store)
                {
                    using (var session = store.OpenSession())
                    {
                        var val = session.TimeSeriesFor("users/ayende", "Heartrate")
                            .Get(DateTime.MinValue, DateTime.MaxValue)
                            .Single();

                        Assert.Equal(new[] { 70d }, val.Values);
                        Assert.Equal("watches/fitbit", val.Tag);
                        Assert.Equal(baseline.AddMinutes(1), val.Timestamp);
                    }
                }

            }
        }

        [Fact]
        public async Task MergeTimeSeriesOnConflict()
        {
            using (var storeA = GetDocumentStore())
            using (var storeB = GetDocumentStore())
            {
                using (var session = storeA.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Karmel" }, "users/1");
                    session.TimeSeriesFor("users/1", "heartbeat").Append(DateTime.Now, new List<double> {1, 2, 3}, "herz");
                    await session.SaveChangesAsync();
                }
                using (var session = storeB.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Karmel" }, "users/1");
                    session.TimeSeriesFor("users/1", "pulse").Append(DateTime.Now, new List<double> {1, 2, 3}, "bps");
                    await session.SaveChangesAsync();
                }

                await SetupReplicationAsync(storeA, storeB);
                EnsureReplicating(storeA, storeB);

                using (var session = storeB.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>("users/1");

                    var flags = session.Advanced.GetMetadataFor(user)[Constants.Documents.Metadata.Flags];
                    Assert.Equal((DocumentFlags.HasTimeSeries).ToString(), flags);
                    var list = session.Advanced.GetTimeSeriesFor(user);
                    Assert.Equal(2, list.Count);
                }
            }
        }

        [Fact]
        public async Task MergeTimeSeriesWithCountersOnConflict()
        {
            using (var storeA = GetDocumentStore())
            using (var storeB = GetDocumentStore())
            {
                using (var session = storeA.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Karmel" }, "users/1");
                    session.TimeSeriesFor("users/1", "heartbeat").Append(DateTime.Now, new List<double> {1, 2, 3}, "herz");
                    await session.SaveChangesAsync();
                }
                using (var session = storeB.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Karmel" }, "users/1");
                    session.CountersFor("users/1").Increment("likes", 10);
                    await session.SaveChangesAsync();
                }

                await SetupReplicationAsync(storeA, storeB);
                EnsureReplicating(storeA, storeB);

                using (var session = storeB.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>("users/1");

                    var flags = session.Advanced.GetMetadataFor(user)[Constants.Documents.Metadata.Flags];
                    Assert.Equal((DocumentFlags.HasTimeSeries | DocumentFlags.HasCounters).ToString(), flags);

                    var ts = session.Advanced.GetTimeSeriesFor(user);
                    Assert.Equal(1, ts.Count);

                    
                    var counters = session.Advanced.GetCountersFor(user);
                    Assert.Equal(1, counters.Count);
                }
            }
        }
    }
}
