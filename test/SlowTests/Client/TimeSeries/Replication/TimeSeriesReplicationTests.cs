using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using FastTests.Server.Replication;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;
using SlowTests.Core.Utils.Entities;
using Sparrow;
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
                var baseline = RavenTestHelper.UtcToday;

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
                    Assert.Equal(baseline.AddMinutes(1), val.Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                }
            }
        }

        [Fact]
        public async Task CanSplitAndMergeLargeRange()
        {
            using (var storeA = GetDocumentStore())
            using (var storeB = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = storeA.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, "users/ayende");
                    for (int i = 0; i < 100; i++)
                    {
                        session.TimeSeriesFor("users/ayende", "Heartrate")
                            .Append(baseline.AddDays(i), new[] { 59d }, "watches/fitbit");
                    }
                   
                    session.SaveChanges();
                }


                using (var session = storeB.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, "users/ayende");
                    for (int i = 0; i < 100; i++)
                    {
                        session.TimeSeriesFor("users/ayende", "Heartrate")
                            .Append(baseline.AddDays(-10).AddDays(i), new[] { 60d }, "watches/fitbit");
                    }
                   
                    session.SaveChanges();
                }

                await SetupReplicationAsync(storeA, storeB);

                WaitForUserToContinueTheTest(storeB);

                EnsureReplicating(storeA, storeB);

                using (var session = storeB.OpenSession())
                {
                    var val = session.TimeSeriesFor("users/ayende", "Heartrate")
                        .Get()
                        .ToList();
                    Assert.Equal(110, val.Count);
                }
            }
        }

        [Fact]
        public async Task TimeSeriesShouldBeCaseInsensitiveAndKeepOriginalCasing()
        {
            using (var storeA = GetDocumentStore())
            using (var storeB = GetDocumentStore())
            {
                await SetupReplicationAsync(storeA, storeB);

                var baseline = RavenTestHelper.UtcToday;

                using (var session = storeA.OpenSession())
                {
                    session.Store(new User { Name = "Oren" }, "users/ayende");

                    session.TimeSeriesFor("users/ayende", "Heartrate")
                        .Append(baseline.AddMinutes(1), 59d, "watches/fitbit");

                    session.SaveChanges();
                }

                using (var session = storeA.OpenSession())
                {
                    session.TimeSeriesFor("users/ayende", "HeartRate")
                        .Append(baseline.AddMinutes(2), 60d, "watches/fitbit");

                    session.SaveChanges();
                }

                EnsureReplicating(storeA, storeB);

                Validate1(storeA, baseline);
                Validate1(storeB, baseline);

                using (var session = storeA.OpenSession())
                {
                    session.TimeSeriesFor("users/ayende", "HeartRatE").Delete();
                    session.SaveChanges();
                }

                using (var session = storeA.OpenSession())
                {
                    session.TimeSeriesFor("users/ayende", "HeArtRate")
                        .Append(baseline.AddMinutes(3), 61d, "watches/fitbit");

                    session.SaveChanges();
                }

                EnsureReplicating(storeA, storeB);

                await Validate2(storeA, baseline);
                await Validate2(storeB, baseline);
            }

            void Validate1(DocumentStore storeA, DateTime baseline)
            {
                using (var session = storeA.OpenSession())
                {
                    var user = session.Load<User>("users/ayende");
                    var val = session.TimeSeriesFor("users/ayende", "heartrate").Get().ToList();

                    Assert.Equal(new[] {59d}, val[0].Values);
                    Assert.Equal("watches/fitbit", val[0].Tag);
                    Assert.Equal(baseline.AddMinutes(1), val[0].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    Assert.Equal(new[] {60d}, val[1].Values);
                    Assert.Equal("watches/fitbit", val[1].Tag);
                    Assert.Equal(baseline.AddMinutes(2), val[1].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    Assert.Equal("Heartrate", session.Advanced.GetTimeSeriesFor(user).Single());
                }
            }

            async Task Validate2(DocumentStore storeA, DateTime baseline)
            {
                using (var session = storeA.OpenSession())
                {
                    var user = session.Load<User>("users/ayende");
                    var val = session.TimeSeriesFor("users/ayende", "heartrate").Get().Single();

                    Assert.Equal(new[] {61d}, val.Values);
                    Assert.Equal("watches/fitbit", val.Tag);
                    Assert.Equal(baseline.AddMinutes(3), val.Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    Assert.Equal("HeArtRate", session.Advanced.GetTimeSeriesFor(user).Single());
                }

                var database = await GetDocumentDatabaseInstanceFor(storeA);
                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var name = database.DocumentsStorage.TimeSeriesStorage.Stats.GetTimeSeriesNamesForDocumentOriginalCasing(ctx, "users/ayende").Single();
                    Assert.Equal("HeArtRate", name);
                }
            }
        }

        [Fact]
        public async Task CanReplicateValuesOutOfOrder()
        {
            using (var storeA = GetDocumentStore())
            using (var storeB = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

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
                        Assert.Equal(baseline.AddMinutes(offset), vals[i].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                        Assert.Equal(offset, vals[i].Values[0]);

                        offset++;
                        i++;

                        Assert.Equal(baseline.AddMinutes(offset), vals[i].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
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
                        Assert.Equal(baseline.AddMinutes(offset), vals[i].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                        Assert.Equal(offset, vals[i].Values[0]);

                        offset++;
                        i++;

                        Assert.Equal(baseline.AddMinutes(offset), vals[i].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
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
                var baseline = RavenTestHelper.UtcToday;

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
                        Assert.Equal(valsA[i].Timestamp, valsB[i].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

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
        public async Task CanReplicateFullSegment2()
        {
            using (var storeA = GetDocumentStore())
            using (var storeB = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                var fullSegment = 329;

                using (var session = storeA.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, "users/ayende");
                    session.SaveChanges();

                    // seg1
                    for (int j = 0; j < fullSegment; j++)
                    {
                        session.TimeSeriesFor("users/ayende", "Heartrate")
                            .Append(baseline.AddMinutes(j), new double[] {j * 2, j * 2, j * 2, j * 2, j * 2}, "watches/fitbit");
                    }
                    session.SaveChanges();

                    // seg2
                    session.TimeSeriesFor("users/ayende", "Heartrate")
                        .Append(baseline.AddMinutes(fullSegment), new double[] {fullSegment *2, fullSegment*2, fullSegment*2, fullSegment*2, fullSegment*2}, "watches/fitbit");
                    session.SaveChanges();

                    var stats = storeA.Maintenance.Send(new GetStatisticsOperation());
                    Assert.Equal(2, stats.CountOfTimeSeriesSegments);
                }

                using (var session = storeB.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, "users/ayende");
                    session.SaveChanges();

                    // seg1
                    for (int j = 0; j < fullSegment - 1; j++)
                    {
                        session.TimeSeriesFor("users/ayende", "Heartrate")
                            .Append(baseline.AddMinutes(j).AddMinutes(1), new double[] {j * 2, j * 2, j * 2, j * 2, j * 2}, "watches/fitbit");
                    }

                    var last = (fullSegment - 2) * 2;
                    session.TimeSeriesFor("users/ayende", "Heartrate")
                        .Append(baseline.AddMinutes(fullSegment).AddMinutes(2), new double[] {last, last, last, last, last}, "watches/fitbit");

                    session.SaveChanges();

                    var stats = storeB.Maintenance.Send(new GetStatisticsOperation());
                    Assert.Equal(1, stats.CountOfTimeSeriesSegments);
                }

                await SetupReplicationAsync(storeB, storeA);
                EnsureReplicating(storeB, storeA);

                await SetupReplicationAsync(storeA, storeB);
                EnsureReplicating(storeA, storeB);

                using (var sessionA = storeA.OpenSession())
                using (var sessionB = storeB.OpenSession())
                {
                    var valsA = sessionA.TimeSeriesFor("users/ayende", "Heartrate").Get().ToList();
                    var valsB = sessionB.TimeSeriesFor("users/ayende", "Heartrate").Get().ToList();

                    Assert.Equal(valsA.Count, valsB.Count);
                }
            }
        }

        [Fact]
        public async Task CanReplicateMany()
        {
            using (var storeA = GetDocumentStore())
            using (var storeB = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

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
                        Assert.Equal(valsB[i].Timestamp, valsA[i].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                        
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
                var baseline = RavenTestHelper.UtcToday;

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

                            tsf.Delete(baseline.AddMinutes(i + 0.5), baseline.AddMinutes(i + 10.5));

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
                        Assert.Equal(valsB[i].Timestamp, valsA[i].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                        Assert.Equal(valsB[i].Values.Length, valsA[i].Values.Length);
                        for (int j = 0; j < valsA[i].Values.Length; j++)
                        {
                            Assert.Equal(valsB[i].Values[j], valsA[i].Values[j]);
                        }
                    }
                }

                await EnsureNoReplicationLoop(Server, storeA.Database);
                await EnsureNoReplicationLoop(Server, storeB.Database);
            }
        }

        [Fact]
        public async Task CanReplicateDeletions()
        {
            using (var storeA = GetDocumentStore())
            using (var storeB = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

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
                        .Delete(baseline.AddMinutes(10));
                    session.SaveChanges();
                }

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
                        Assert.Equal(valsB[i].Timestamp, valsA[i].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

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
        public async Task CanReplicateDeadSegment()
        {
            using (var storeA = GetDocumentStore())
            using (var storeB = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = storeA.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, "users/ayende");
                    session.TimeSeriesFor("users/ayende", "Heartrate")
                        .Append(baseline.AddMinutes(10), new double[] { 1 }, "watches/fitbit");
                    session.SaveChanges();
                }

                await SetupReplicationAsync(storeA, storeB);
                EnsureReplicating(storeA, storeB);

                using (var session = storeA.OpenSession())
                {
                    session.TimeSeriesFor("users/ayende", "Heartrate").Delete();
                    session.SaveChanges();
                }

                EnsureReplicating(storeA, storeB);
                
                var a = await GetDocumentDatabaseInstanceFor(storeA);
                await AssertNoLeftOvers(a);

                var b = await GetDocumentDatabaseInstanceFor(storeB);
                await AssertNoLeftOvers(b);
            }
        }

        [Fact]
        public async Task CanReplicateDeadSegment2()
        {
            using (var storeA = GetDocumentStore())
            using (var storeB = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = storeA.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, "users/ayende");
                    session.TimeSeriesFor("users/ayende", "Heartrate")
                        .Append(baseline.AddMinutes(10), new double[] { 1 }, "watches/fitbit");
                    session.SaveChanges();
                }

                await SetupReplicationAsync(storeA, storeB);
                EnsureReplicating(storeA, storeB);

                using (var session = storeA.OpenSession())
                {
                    session.TimeSeriesFor("users/ayende", "Heartrate").Delete();
                    session.TimeSeriesFor("users/ayende", "Heartrate2")
                        .Append(baseline.AddMinutes(10), new double[] { 1 }, "watches/fitbit");
                    session.SaveChanges();
                }

                EnsureReplicating(storeA, storeB);

                using (var session = storeB.OpenSession())
                {
                    var user = session.Load<dynamic>("users/ayende");
                    List<string> names = session.Advanced.GetTimeSeriesFor(user);
                    Assert.Equal(1, names.Count);
                }
            }
        }

        [Fact]
        public async Task ReplicatingDeletedDocumentShouldRemoveTimeseries()
        {
            using (var storeA = GetDocumentStore())
            using (var storeB = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = storeA.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, "users/ayende");
                    session.TimeSeriesFor("users/ayende", "Heartrate")
                        .Append(baseline.AddMinutes(10), new double[] { 1 }, "watches/fitbit");
                    session.SaveChanges();
                }

                await SetupReplicationAsync(storeA, storeB);
                EnsureReplicating(storeA, storeB);

                using (var session = storeA.OpenSession())
                {
                    session.Delete("users/ayende");
                    session.SaveChanges();
                }

                EnsureReplicating(storeA, storeB);
                
                var a = await GetDocumentDatabaseInstanceFor(storeA);
                await AssertNoLeftOvers(a);

                var b = await GetDocumentDatabaseInstanceFor(storeB);
                await AssertNoLeftOvers(b);
            }
        }

        public static async Task AssertNoLeftOvers(DocumentDatabase db)
        {
            await db.TombstoneCleaner.ExecuteCleanup();
            using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                Assert.Equal(0, db.DocumentsStorage.TimeSeriesStorage.GetNumberOfTimeSeriesSegments(ctx));
                Assert.Equal(0, db.DocumentsStorage.TimeSeriesStorage.Stats.GetNumberOfEntries(ctx));
            }
        }

        [Fact]
        public async Task PreferDeletedValues()
        {
            using (var storeA = GetDocumentStore())
            using (var storeB = GetDocumentStore())
            {
                var baseline = DateTime.UtcNow.EnsureMilliseconds();

                using (var session = storeA.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, "users/ayende");
                    session.TimeSeriesFor("users/ayende", "Heartrate").Append(baseline, new[] { 59d }, "watches/fitbit");
                    session.TimeSeriesFor("users/ayende", "Heartrate").Append(baseline.AddMinutes(1), new[] { 60d }, "watches/fitbit");
                    session.SaveChanges();
                }

                using (var session = storeB.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, "users/ayende");
                    session.TimeSeriesFor("users/ayende", "Heartrate").Append(baseline, new[] { 70d }, "watches/fitbit2");
                    session.SaveChanges();
                }

                using (var session = storeB.OpenSession())
                {
                    session.TimeSeriesFor("users/ayende", "Heartrate").Delete(baseline);
                    session.SaveChanges();
                }

                await SetupReplicationAsync(storeA, storeB);
                EnsureReplicating(storeA, storeB);

                await SetupReplicationAsync(storeB, storeA);
                EnsureReplicating(storeB, storeA);

                AssertValues(storeA);
                AssertValues(storeB);

                await EnsureNoReplicationLoop(Server, storeA.Database);
                await EnsureNoReplicationLoop(Server, storeB.Database);

                void AssertValues(IDocumentStore store)
                {
                    using (var session = store.OpenSession())
                    {
                        var val = session.TimeSeriesFor("users/ayende", "Heartrate").Get().Single();

                        Assert.Equal(60, val.Value);
                        Assert.Equal("watches/fitbit", val.Tag);
                        Assert.Equal(baseline.AddMinutes(1), val.Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                    }
                }
            }
        }

        [Fact]
        public async Task PreferDeletedValues2()
        {
            using (var storeA = GetDocumentStore())
            using (var storeB = GetDocumentStore())
            {
                var baseline = DateTime.UtcNow.EnsureMilliseconds();

                using (var session = storeB.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, "users/ayende");
                    session.TimeSeriesFor("users/ayende", "Heartrate").Append(baseline, new[] { 70d }, "watches/fitbit2");
                    session.SaveChanges();
                }

                using (var session = storeB.OpenSession())
                {
                    session.TimeSeriesFor("users/ayende", "Heartrate").Delete(baseline);
                    session.SaveChanges();
                }

                using (var session = storeA.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, "users/ayende");
                    session.TimeSeriesFor("users/ayende", "Heartrate").Append(baseline, new[] { 59d }, "watches/fitbit");
                    session.TimeSeriesFor("users/ayende", "Heartrate").Append(baseline.AddMinutes(1), new[] { 60d }, "watches/fitbit");
                    session.SaveChanges();
                }

                await SetupReplicationAsync(storeA, storeB);
                EnsureReplicating(storeA, storeB);

                await SetupReplicationAsync(storeB, storeA);
                EnsureReplicating(storeB, storeA);

                AssertValues(storeA);
                AssertValues(storeB);

                await EnsureNoReplicationLoop(Server, storeA.Database);
                await EnsureNoReplicationLoop(Server, storeB.Database);

                void AssertValues(IDocumentStore store)
                {
                    using (var session = store.OpenSession())
                    {
                        var val = session.TimeSeriesFor("users/ayende", "Heartrate").Get().Single();

                        Assert.Equal(60, val.Value);
                        Assert.Equal("watches/fitbit", val.Tag);
                        Assert.Equal(baseline.AddMinutes(1), val.Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                    }
                }
            }
        }

        [Fact]
        public async Task PreferDeletedValues3()
        {
            using (var storeA = GetDocumentStore())
            using (var storeB = GetDocumentStore())
            {
                var baseline = DateTime.UtcNow.EnsureMilliseconds();
                
                using (var session = storeA.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, "users/ayende");
                    session.TimeSeriesFor("users/ayende", "Heartrate").Append(baseline, new[] { 59d,60d }, "watches/fitbit");
                    session.TimeSeriesFor("users/ayende", "Heartrate").Append(baseline.AddMinutes(1), new[] { 60d,61d }, "watches/fitbit");
                    session.SaveChanges();
                }

                using (var session = storeB.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, "users/ayende");
                    session.TimeSeriesFor("users/ayende", "Heartrate").Append(baseline, new[] { 70d }, "watches/fitbit2");
                    session.SaveChanges();
                }

                using (var session = storeB.OpenSession())
                {
                    session.TimeSeriesFor("users/ayende", "Heartrate").Delete(baseline);
                    session.SaveChanges();
                }
                await SetupReplicationAsync(storeB, storeA);
                await SetupReplicationAsync(storeA, storeB);
                
                EnsureReplicating(storeA, storeB);
                EnsureReplicating(storeB, storeA);

                AssertValues(storeA);
                AssertValues(storeB);

                await EnsureNoReplicationLoop(Server, storeA.Database);
                await EnsureNoReplicationLoop(Server, storeB.Database);

                void AssertValues(IDocumentStore store)
                {
                    using (var session = store.OpenSession())
                    {
                        var val = session.TimeSeriesFor("users/ayende", "Heartrate").Get().Single();

                        Assert.Equal(60, val.Values[0]);
                        Assert.Equal(61, val.Values[1]);
                        Assert.Equal("watches/fitbit", val.Tag);
                        Assert.Equal(baseline.AddMinutes(1), val.Timestamp, RavenTestHelper.DateTimeComparer.Instance);
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
                var baseline = RavenTestHelper.UtcToday;

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
                        Assert.Equal(baseline.AddMinutes(1), val.Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                        val = values[1];
                        Assert.Equal(new[] { 70d }, val.Values);
                        Assert.Equal("watches/fitbit2", val.Tag);
                        Assert.Equal(baseline.AddMinutes(2), val.Timestamp, RavenTestHelper.DateTimeComparer.Instance);
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
                var baseline = RavenTestHelper.UtcToday;

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
                        Assert.Equal(baseline.AddMinutes(1), val.Timestamp, RavenTestHelper.DateTimeComparer.Instance);
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
