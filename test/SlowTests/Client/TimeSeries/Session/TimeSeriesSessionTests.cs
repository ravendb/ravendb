using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Documents.Queries.TimeSeries;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Tests.Core.Utils.Entities;
using Sparrow;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client.TimeSeries.Session
{
    public class TimeSeriesSessionTests : RavenTestBase
    {
        public TimeSeriesSessionTests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanCreateSimpleTimeSeries()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, "users/ayende");

                    session.TimeSeriesFor("users/ayende", "Heartrate")
                        .Append(baseline.AddMinutes(1), 59d, "watches/fitbit");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
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
        public void CanCreateSimpleTimeSeries2()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, "users/ayende");

                    var tsf = session.TimeSeriesFor("users/ayende", "Heartrate");

                    tsf.Append(baseline.AddMinutes(1), new[] { 59d }, "watches/fitbit");
                    tsf.Append(baseline.AddMinutes(2), new[] { 60d }, "watches/fitbit");
                    tsf.Append(baseline.AddMinutes(2), new[] { 61d }, "watches/fitbit");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var val = session.TimeSeriesFor("users/ayende", "Heartrate")
                        .Get(DateTime.MinValue, DateTime.MaxValue)
                        .ToList();
                    Assert.Equal(2, val.Count);
                }
            }
        }

        [Fact]
        public async Task TimeSeriesShouldBeCaseInsensitiveAndKeepOriginalCasing()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Oren" }, "users/ayende");

                    session.TimeSeriesFor("users/ayende", "Heartrate")
                        .Append(baseline.AddMinutes(1), 59d, "watches/fitbit");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.TimeSeriesFor("users/ayende", "HeartRate")
                        .Append(baseline.AddMinutes(2), 60d, "watches/fitbit");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>("users/ayende");
                    var val = session.TimeSeriesFor("users/ayende", "heartrate").Get().ToList();
                        
                    Assert.Equal(new[] { 59d }, val[0].Values);
                    Assert.Equal("watches/fitbit", val[0].Tag);
                    Assert.Equal(baseline.AddMinutes(1), val[0].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    Assert.Equal(new[] { 60d }, val[1].Values);
                    Assert.Equal("watches/fitbit", val[1].Tag);
                    Assert.Equal(baseline.AddMinutes(2), val[1].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    Assert.Equal("Heartrate", session.Advanced.GetTimeSeriesFor(user).Single());
                }


                using (var session = store.OpenSession())
                {
                    session.TimeSeriesFor("users/ayende", "HeartRatE").Delete();
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.TimeSeriesFor("users/ayende", "HeArtRate")
                        .Append(baseline.AddMinutes(3), 61d, "watches/fitbit");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>("users/ayende");
                    var val = session.TimeSeriesFor("users/ayende", "heartrate").Get().Single();
                        
                    Assert.Equal(new[] { 61d }, val.Values);
                    Assert.Equal("watches/fitbit", val.Tag);
                    Assert.Equal(baseline.AddMinutes(3), val.Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    Assert.Equal("HeArtRate", session.Advanced.GetTimeSeriesFor(user).Single());
                }

                var database = await GetDocumentDatabaseInstanceFor(store);
                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var name = database.DocumentsStorage.TimeSeriesStorage.Stats.GetTimeSeriesNamesForDocumentOriginalCasing(ctx, "users/ayende").Single();
                    Assert.Equal("HeArtRate", name);
                }
            }
        }

        [Fact]
        public async Task ThrowIfAppendIsLessThen1Ms()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.UtcNow.EnsureMilliseconds();

                using (var session = store.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, "users/ayende");
                    session.SaveChanges();
                }

                var database = await GetDocumentDatabaseInstanceFor(store);
                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                using (ctx.OpenWriteTransaction())
                {
                    var appends = new[]
                    {
                        new SingleResult {Timestamp = baseline.AddMinutes(1), Tag = ctx.GetLazyString("watches/fitbit"), Values = new Memory<double>(new double[] {59})},
                        new SingleResult {Timestamp = baseline.AddMinutes(1).AddTicks(10), Tag = ctx.GetLazyString("watches/fitbit"), Values = new Memory<double>(new double[] {60})},
                        new SingleResult {Timestamp = baseline.AddMinutes(1).AddTicks(20), Tag = ctx.GetLazyString("watches/fitbit"), Values = new Memory<double>(new double[] {61})},
                    };

                    var e = Assert.Throws<InvalidDataException>(() => database.DocumentsStorage.TimeSeriesStorage.AppendTimestamp(ctx, "users/ayende", "users", "heartrate", appends));
                    Assert.Contains("must be sorted by their timestamps, and cannot contain duplicate timestamps.", e.Message);
                }
            }
        }

        [Fact]
        public void CanDeleteTimestamp2()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Oren" }, "users/ayende");

                    var tsf = session.TimeSeriesFor("users/ayende", "Heartrate");

                    tsf.Append(baseline.AddMinutes(1), 59d);
                    tsf.Append(baseline.AddMinutes(2), 69d);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.TimeSeriesFor("users/ayende", "Heartrate").Delete(baseline.AddMinutes(2));
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesAggregationResult>($@"
                    declare timeseries out(x) 
                    {{
                        from x.HeartRate between $start and $end
                        group by 1h
                        select last()
                    }}
                    from Users as u
                    select out(u)
                    ")
                        .AddParameter("start", baseline.EnsureUtc())
                        .AddParameter("end", baseline.AddMonths(2).EnsureUtc());

                    var result = query.ToList();
                    var last = result[0].Results[0].Last[0];
                    Assert.Equal(59, last);
                }
            }
        }
        
        [Fact]
        public void CanDeleteTimestamp3()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Oren" }, "users/ayende");

                    var tsf = session.TimeSeriesFor("users/ayende", "Heartrate");

                    tsf.Append(baseline.AddMinutes(1), 59d);
                    tsf.Append(baseline.AddMinutes(2), 69d);
                    tsf.Append(baseline.AddMinutes(3), 79d);
                    tsf.Append(baseline.AddMinutes(4), 89d);
                    tsf.Append(baseline.AddMinutes(5), 99d);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.TimeSeriesFor("users/ayende", "Heartrate").Delete(baseline.AddMinutes(2));
                    session.TimeSeriesFor("users/ayende", "Heartrate").Delete(baseline.AddMinutes(3));
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var entries = session.TimeSeriesFor("users/ayende", "Heartrate").Get();
                    Assert.Equal(59,entries[0].Values[0]);
                    Assert.Equal(89,entries[1].Values[0]);
                    Assert.Equal(99, entries[2].Values[0]);
                }

                using (var session = store.OpenSession())
                {
                    session.TimeSeriesFor("users/ayende", "Heartrate").Delete(baseline.AddMinutes(4));
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var entries = session.TimeSeriesFor("users/ayende", "Heartrate").Get();
                    Assert.Equal(59,entries[0].Values[0]);
                    Assert.Equal(99, entries[1].Values[0]);
                }
            }
        }

        [Fact]
        public void CanDeleteTimestamp4()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Oren" }, "users/ayende");

                    var tsf = session.TimeSeriesFor("users/ayende", "Heartrate");

                    tsf.Append(baseline.AddMinutes(1), 59d);
                    tsf.Append(baseline.AddMinutes(2), 69d);
                    tsf.Append(baseline.AddMinutes(3), 79d);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.TimeSeriesFor("users/ayende", "Heartrate").Delete(baseline.AddMinutes(2));
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var entries = session.TimeSeriesFor("users/ayende", "Heartrate").Get();
                    Assert.Equal(59,entries[0].Values[0]);
                    Assert.Equal(79,entries[1].Values[0]);
                }
            }
        }

        [Fact]
        public void CanDeleteTimestamp()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, "users/ayende");

                    var tsf = session.TimeSeriesFor("users/ayende", "Heartrate");

                    tsf.Append(baseline.AddMinutes(1), new[] { 59d }, "watches/fitbit");
                    tsf.Append(baseline.AddMinutes(2), new[] { 69d }, "watches/fitbit");
                    tsf.Append(baseline.AddMinutes(3), new[] { 79d }, "watches/fitbit");


                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, "users/ayende");
                    session.TimeSeriesFor("users/ayende", "Heartrate")
                        .Delete(baseline.AddMinutes(2));

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var vals = session.TimeSeriesFor("users/ayende", "Heartrate")
                         .Get(DateTime.MinValue, DateTime.MaxValue)
                         .ToList();
                    Assert.Equal(2, vals.Count);
                    Assert.Equal(new[] { 59d }, vals[0].Values);
                    Assert.Equal("watches/fitbit", vals[0].Tag);
                    Assert.Equal(baseline.AddMinutes(1), vals[0].Timestamp, RavenTestHelper.DateTimeComparer.Instance);;

                    Assert.Equal(new[] { 79d }, vals[1].Values);
                    Assert.Equal("watches/fitbit", vals[1].Tag);
                    Assert.Equal(baseline.AddMinutes(3), vals[1].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                }
            }
        }

        [Fact]
        public void UsingDifferentTags()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, "users/ayende");

                    var tsf = session.TimeSeriesFor("users/ayende", "Heartrate");
                    tsf.Append(baseline.AddMinutes(1), new[] { 59d }, "watches/fitbit");
                    tsf.Append(baseline.AddMinutes(2), new[] { 70d }, "watches/apple");
                    
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var vals = session.TimeSeriesFor("users/ayende", "Heartrate")
                        .Get(DateTime.MinValue, DateTime.MaxValue)
                        .ToList();
                    Assert.Equal(2, vals.Count);
                    Assert.Equal(new[] { 59d }, vals[0].Values);
                    Assert.Equal("watches/fitbit", vals[0].Tag);
                    Assert.Equal(baseline.AddMinutes(1), vals[0].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    Assert.Equal(new[] { 70d }, vals[1].Values);
                    Assert.Equal("watches/apple", vals[1].Tag);
                    Assert.Equal(baseline.AddMinutes(2), vals[1].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                }
            }
        }

        [Fact]
        public void UsingDifferentNumberOfValues_SmallToLarge()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, "users/ayende");

                    var tsf = session.TimeSeriesFor("users/ayende", "Heartrate");
                    tsf.Append(baseline.AddMinutes(1), new[] { 59d }, "watches/fitbit");
                    tsf.Append(baseline.AddMinutes(2), new[] { 70d, 120d, 80d }, "watches/apple");
                    tsf.Append(baseline.AddMinutes(3), new[] { 69d }, "watches/fitbit");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var vals = session.TimeSeriesFor("users/ayende", "Heartrate")
                        .Get(DateTime.MinValue, DateTime.MaxValue)
                        .ToList();
                    Assert.Equal(3, vals.Count);
                    Assert.Equal(new[] { 59d }, vals[0].Values);
                    Assert.Equal("watches/fitbit", vals[0].Tag);
                    Assert.Equal(baseline.AddMinutes(1), vals[0].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    Assert.Equal(new[] { 70d, 120d, 80d }, vals[1].Values);
                    Assert.Equal("watches/apple", vals[1].Tag);
                    Assert.Equal(baseline.AddMinutes(2), vals[1].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    Assert.Equal(new[] { 69d }, vals[2].Values);
                    Assert.Equal("watches/fitbit", vals[2].Tag);
                    Assert.Equal(baseline.AddMinutes(3), vals[2].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                }
            }
        }

        [Fact]
        public void UsingDifferentNumberOfValues_SmallToLargeSplit()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, "users/ayende");

                    var tsf = session.TimeSeriesFor("users/ayende", "Heartrate");
                    tsf.Append(baseline.AddMinutes(1), new[] { 59d });
                    tsf.Append(baseline.AddMinutes(4), new[] { 89d });
                    tsf.Append(baseline.AddMinutes(5), new[] { 99d });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, "users/ayende");

                    var tsf = session.TimeSeriesFor("users/ayende", "Heartrate");
                    tsf.Append(baseline.AddMinutes(2), new[] { 69d });
                    tsf.Append(baseline.AddMinutes(3), new[] { 79d, 666d });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var vals = session.TimeSeriesFor("users/ayende", "Heartrate").Get().ToList();

                    Assert.Equal(5, vals.Count);
                    Assert.Equal(new[] { 59d }, vals[0].Values);
                    Assert.Equal(baseline.AddMinutes(1), vals[0].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    Assert.Equal(new[] { 69d }, vals[1].Values);
                    Assert.Equal(baseline.AddMinutes(2), vals[1].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    Assert.Equal(new[] { 79d, 666d }, vals[2].Values);
                    Assert.Equal(baseline.AddMinutes(3), vals[2].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    Assert.Equal(new[] { 89d }, vals[3].Values);
                    Assert.Equal(baseline.AddMinutes(4), vals[3].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    Assert.Equal(new[] { 99d }, vals[4].Values);
                    Assert.Equal(baseline.AddMinutes(5), vals[4].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                }
            }
        }

        [Fact]
        public void UsingDifferentNumberOfValues_LargeToSmall()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, "users/ayende");

                    var tsf = session.TimeSeriesFor("users/ayende", "Heartrate");
                    tsf.Append(baseline.AddMinutes(1), new[] { 70d, 120d, 80d }, "watches/apple");
                    tsf.Append(baseline.AddMinutes(2), new[] { 59d }, "watches/fitbit");
                    tsf.Append(baseline.AddMinutes(3), new[] { 69d }, "watches/fitbit");

                    session.SaveChanges();
                }


                using (var session = store.OpenSession())
                {
                    var vals = session.TimeSeriesFor("users/ayende", "Heartrate")
                        .Get(DateTime.MinValue, DateTime.MaxValue)
                        .ToList();
                    Assert.Equal(3, vals.Count);

                    Assert.Equal(new[] { 70d, 120d, 80d }, vals[0].Values);
                    Assert.Equal("watches/apple", vals[0].Tag);
                    Assert.Equal(baseline.AddMinutes(1), vals[0].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    Assert.Equal(new[] { 59d }, vals[1].Values);
                    Assert.Equal("watches/fitbit", vals[1].Tag);
                    Assert.Equal(baseline.AddMinutes(2), vals[1].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    Assert.Equal(new[] { 69d }, vals[2].Values);
                    Assert.Equal("watches/fitbit", vals[2].Tag);
                    Assert.Equal(baseline.AddMinutes(3), vals[2].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                }
            }
        }

        [Fact]
        public void CanStoreAndReadMultipleTimestamps()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, "users/ayende");
                    session.TimeSeriesFor("users/ayende", "Heartrate")
                        .Append(baseline.AddMinutes(1), new[] { 59d }, "watches/fitbit");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var tsf = session.TimeSeriesFor("users/ayende", "Heartrate");
                    tsf.Append(baseline.AddMinutes(2), new[] { 61d }, "watches/fitbit");
                    tsf.Append(baseline.AddMinutes(3), new[] { 62d }, "watches/apple-watch");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var vals = session.TimeSeriesFor("users/ayende", "Heartrate")
                        .Get(DateTime.MinValue, DateTime.MaxValue)
                        .ToList();
                    Assert.Equal(3, vals.Count);

                    Assert.Equal(new[] { 59d }, vals[0].Values);
                    Assert.Equal("watches/fitbit", vals[0].Tag);
                    Assert.Equal(baseline.AddMinutes(1), vals[0].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    Assert.Equal(new[] { 61d }, vals[1].Values);
                    Assert.Equal("watches/fitbit", vals[1].Tag);
                    Assert.Equal(baseline.AddMinutes(2), vals[1].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    Assert.Equal(new[] { 62d }, vals[2].Values);
                    Assert.Equal("watches/apple-watch", vals[2].Tag);
                    Assert.Equal(baseline.AddMinutes(3), vals[2].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                }
            }
        }

        [Fact]
        public void CanStoreLargeNumberOfValues()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, "users/ayende");
                    session.SaveChanges();
                }

                var offset = 0;

                for (int i = 0; i < 10; i++)
                {
                    using (var session = store.OpenSession())
                    {
                        var tsf = session.TimeSeriesFor("users/ayende", "Heartrate");

                        for (int j = 0; j < 1000; j++)
                        {
                            tsf.Append(baseline.AddMinutes(offset++), new double[] { offset }, "watches/fitbit");
                        }

                        session.SaveChanges();
                    }
                }

                using (var session = store.OpenSession())
                {
                    var vals = session.TimeSeriesFor("users/ayende", "Heartrate")
                        .Get(DateTime.MinValue, DateTime.MaxValue)
                        .ToList();
                    Assert.Equal(10_000, vals.Count);

                    for (int i = 0; i < 10_000; i++)
                    {
                        Assert.Equal(baseline.AddMinutes(i), vals[i].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                        Assert.Equal(1 + i, vals[i].Values[0]);
                    }
                }
            }
        }

        [Fact]
        public void CanStoreValuesOutOfOrder()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, "users/ayende");
                    session.SaveChanges();
                }

                const int retries = 1000;

                var offset = 0;

                using (var session = store.OpenSession())
                {
                    var tsf = session.TimeSeriesFor("users/ayende", "Heartrate");

                    for (int j = 0; j < retries; j++)
                    {
                        tsf.Append(baseline.AddMinutes(offset), new double[] { offset }, "watches/fitbit");

                        offset += 5;
                    }

                    session.SaveChanges();
                }

                offset = 1;

                using (var session = store.OpenSession())
                {
                    var tsf = session.TimeSeriesFor("users/ayende", "Heartrate");
                    var vals = tsf.Get(DateTime.MinValue, DateTime.MaxValue).ToList();
                    Assert.Equal(retries, vals.Count);

                    for (int j = 0; j < retries; j++)
                    {
                        tsf.Append(baseline.AddMinutes(offset), new double[] { offset }, "watches/fitbit");
                        offset += 5;
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
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
        public void CanUseLocalDateTimeWhenRequestingTimeSeriesRange()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, "users/ayende");
                    session.TimeSeriesFor("users/ayende", "Heartrate")
                        .Append(baseline, new[] { 0d }, "watches/fitbit");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var timeSeriesFor = session.TimeSeriesFor("users/ayende", "Heartrate");

                    for (double i = 1; i < 10; i++)
                    {
                        timeSeriesFor.Append(baseline.AddMinutes(i), new[] { i }, "watches/fitbit");
                    }

                    session.SaveChanges();
                }


                for (int i = 0; i < 10; i++)
                {
                    using (var session = store.OpenSession())
                    {
                        var vals = session.TimeSeriesFor("users/ayende", "Heartrate")
                            .Get(baseline.AddMinutes(i), DateTime.MaxValue)
                            .ToList();

                        Assert.Equal(10 - i, vals.Count);

                        for (double j = 0; j < vals.Count; j++)
                        {
                            Assert.Equal(new[] { j + i }, vals[(int)j].Values);
                        }
                    }
                }


                var maxTimeStamp = baseline.AddMinutes(9);

                for (int i = 1; i < 10; i++)
                {
                    using (var session = store.OpenSession())
                    {
                        var vals = session.TimeSeriesFor("users/ayende", "Heartrate")
                            .Get(baseline, maxTimeStamp.AddMinutes(-i))
                            .ToList();

                        Assert.Equal(10 - i, vals.Count);

                        for (double j = 0; j < vals.Count; j++)
                        {
                            Assert.Equal(new[] { j }, vals[(int)j].Values);
                        }
                    }
                }
            }
        }

        [Fact]
        public void CanRequestNonExistingTimeSeriesRange()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, "users/ayende");
                    var tsf = session.TimeSeriesFor("users/ayende", "Heartrate");
                    tsf.Append(baseline, new[] { 58d }, "watches/fitbit");
                    tsf.Append(baseline.AddMinutes(10), new[] { 60d }, "watches/fitbit");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var vals = session.TimeSeriesFor("users/ayende", "Heartrate")
                        .Get(baseline.AddMinutes(-10), baseline.AddMinutes(-5))?
                        .ToList();

                    Assert.Empty(vals);

                    vals = session.TimeSeriesFor("users/ayende", "Heartrate")
                        .Get(baseline.AddMinutes(5), baseline.AddMinutes(9))?
                        .ToList();

                    Assert.Empty(vals);
                }
            }
        }

        internal class CanGetTimeSeriesRangeCases : IEnumerable<object[]>
        {
            private readonly List<object[]> _data = new List<object[]>
            {
                new object[] {null, null, 4},
                new object[] {new DateTime(2020, 1, 1), null, 3},
                new object[] {null, new DateTime(2020, 2, 1), 3},
                new object[] {new DateTime(2020, 1, 1), new DateTime(2020, 2, 1), 2},
            };

            public IEnumerator<object[]> GetEnumerator() => _data.GetEnumerator();

            IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
        }

        [Theory]
        [ClassData(typeof(CanGetTimeSeriesRangeCases))]
        public void CanGetTimeSeriesRange(DateTime? from, DateTime? to, int expectedValue)
        {
            using (var store = GetDocumentStore())
            {
                var baseline = new DateTime(2019, 12, 1);

                using (var session = store.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, "users/ayende");
                    var tsf = session.TimeSeriesFor("users/ayende", "Heartrate");
                    tsf.Append(baseline, new[] { 58d }, "watches/fitbit");
                    tsf.Append(baseline.AddMonths(1), new[] { 60d }, "watches/fitbit");
                    tsf.Append(baseline.AddMonths(2), new[] { 60d }, "watches/fitbit");
                    tsf.Append(baseline.AddMonths(3), new[] { 60d }, "watches/fitbit");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var vals = session.TimeSeriesFor("users/ayende", "Heartrate")
                        .Get(from, to)
                        .ToList();

                    Assert.Equal(expectedValue, vals.Count);
                }
            }
        }

        [Fact]
        public void CanGetMultipleTimeSeriesRange()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = new DateTime(2019, 12, 1);

                using (var session = store.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, "users/ayende");
                    var tsf = session.TimeSeriesFor("users/ayende", "Heartrate");
                    tsf.Append(baseline, new[] { 58d }, "watches/fitbit");
                    tsf.Append(baseline.AddMonths(1), new[] { 60d }, "watches/fitbit");
                    tsf.Append(baseline.AddMonths(2), new[] { 60d }, "watches/fitbit");
                    tsf.Append(baseline.AddMonths(3), new[] { 60d }, "watches/fitbit");

                    session.SaveChanges();
                }

                var ranges = new CanGetTimeSeriesRangeCases().Select(c=> new TimeSeriesRange
                {
                    From = (DateTime?)c[0],
                    To = (DateTime?)c[1],
                    Name = "Heartrate"
                });

                var results = store.Operations.Send(new GetMultipleTimeSeriesOperation("users/ayende", ranges));
                var param = new CanGetTimeSeriesRangeCases().ToList();
                for (var index = 0; index < param.Count; index++)
                {
                    var range = param[index];
                    var rangeResult = results.Values["Heartrate"][index];
                    Assert.Equal(range[0] ?? DateTime.MinValue, rangeResult.From);
                    Assert.Equal(range[1] ?? DateTime.MaxValue, rangeResult.To);
                    Assert.Equal(range[2], rangeResult.Entries.Length);
                }
            }
        }

        [Fact]
        public void CanGetTimeSeriesNames()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User(), "users/karmel");
                    session.TimeSeriesFor("users/karmel", "Nasdaq2")
                        .Append(DateTime.Now, new[] { 7547.31 }, "web");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.TimeSeriesFor("users/karmel", "Heartrate2")
                        .Append(DateTime.Now, new[] { 7547.31 }, "web");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Store(new User(), "users/ayende");
                    session.TimeSeriesFor("users/ayende", "Nasdaq")
                        .Append(DateTime.Now, new[] { 7547.31 }, "web");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.TimeSeriesFor("users/ayende", "Heartrate")
                        .Append(DateTime.Today.AddMinutes(1), new[] { 58d }, "fitbit");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>("users/ayende");
                    var tsNames = session.Advanced.GetTimeSeriesFor(user);
                    Assert.Equal(2, tsNames.Count);

                    // should be sorted
                    Assert.Equal("Heartrate", tsNames[0]);
                    Assert.Equal("Nasdaq", tsNames[1]);
                }

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>("users/karmel");
                    var tsNames = session.Advanced.GetTimeSeriesFor(user);
                    Assert.Equal(2, tsNames.Count);

                    // should be sorted
                    Assert.Equal("Heartrate2", tsNames[0]);
                    Assert.Equal("Nasdaq2", tsNames[1]);
                }

                using (var session = store.OpenSession())
                {
                    session.TimeSeriesFor("users/ayende", "heartrate")  // putting ts name as lower cased
                        .Append(DateTime.Today.AddMinutes(1), new[] { 58d }, "fitbit");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>("users/ayende");
                    var tsNames = session.Advanced.GetTimeSeriesFor(user);
                    Assert.Equal(2, tsNames.Count);

                    // should preserve original casing
                    Assert.Equal("Heartrate", tsNames[0]);
                    Assert.Equal("Nasdaq", tsNames[1]);
                }
            }
        }

        [Fact]
        public void CanGetTimeSeriesNames2()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today.ToUniversalTime();

                using (var session = store.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, "users/ayende");
                    session.SaveChanges();
                }

                var offset = 0;

                for (int i = 0; i < 100; i++)
                {
                    using (var session = store.OpenSession())
                    {
                        var tsf = session.TimeSeriesFor("users/ayende", "Heartrate");

                        for (int j = 0; j < 1000; j++)
                        {
                            tsf.Append(baseline.AddMinutes(offset++), new double[] { offset }, "watches/fitbit");
                        }

                        session.SaveChanges();
                    }
                }

                offset = 0;

                for (int i = 0; i < 100; i++)
                {
                    using (var session = store.OpenSession())
                    {
                        var tsf = session.TimeSeriesFor("users/ayende", "Pulse");

                        for (int j = 0; j < 1000; j++)
                        {
                            tsf.Append(baseline.AddMinutes(offset++), new double[] { offset }, "watches/fitbit");
                        }

                        session.SaveChanges();
                    }
                }

                using (var session = store.OpenSession())
                {
                    var vals = session.TimeSeriesFor("users/ayende", "Heartrate")
                        .Get(DateTime.MinValue, DateTime.MaxValue)
                        .ToList();
                    Assert.Equal(100_000, vals.Count);

                    for (int i = 0; i < 100_000; i++)
                    {
                        Assert.Equal(baseline.AddMinutes(i), vals[i].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                        Assert.Equal(1 + i, vals[i].Values[0]);
                    }
                }

                using (var session = store.OpenSession())
                {
                    var vals = session.TimeSeriesFor("users/ayende", "Pulse")
                        .Get(DateTime.MinValue, DateTime.MaxValue)
                        .ToList();
                    Assert.Equal(100_000, vals.Count);

                    for (int i = 0; i < 100_000; i++)
                    {
                        Assert.Equal(baseline.AddMinutes(i), vals[i].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                        Assert.Equal(1 + i, vals[i].Values[0]);
                    }
                }

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>("users/ayende");
                    var tsNames = session.Advanced.GetTimeSeriesFor(user);
                    Assert.Equal(2, tsNames.Count);

                    // should be sorted
                    Assert.Equal("Heartrate", tsNames[0]);
                    Assert.Equal("Pulse", tsNames[1]);
                }
            }
        }

        [Fact]
        public void DocumentsChangeVectorShouldBeUpdatedAfterAddingNewTimeSeries()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    for (int i = 1; i <= 5; i++)
                    {
                        var id = $"users/{i}";
                        session.Store(new User
                        {
                            Name = "Oren"
                        }, id);

                        session.TimeSeriesFor(id, "Heartrate")
                            .Append(baseline.AddMinutes(1), new[] { 59d }, "watches/fitbit");
                    }

                    session.SaveChanges();
                }

                var cvs = new List<string>();

                using (var session = store.OpenSession())
                {
                    for (int i = 2; i < 5; i++)
                    {
                        var id = $"users/{i}";
                        var u = session.Load<User>(id);
                        var cv = session.Advanced.GetChangeVectorFor(u);
                        cvs.Add(cv);

                        session.TimeSeriesFor(id, "Nasdaq")
                            .Append(baseline.AddMinutes(1), new[] { 4012.5d }, "web");
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    for (int i = 2; i < 5; i++)
                    {
                        var u = session.Load<User>($"users/{i}");
                        var cv = session.Advanced.GetChangeVectorFor(u);
                        var oldCv = cvs[i - 2];
                        var conflictStatus = ChangeVectorUtils.GetConflictStatus(cv, oldCv);

                        Assert.Equal(ConflictStatus.Update, conflictStatus);
                    }
                }
            }
        }

        [Fact]
        public void CanUseIEnumerableValues()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                IEnumerable<double> values = new List<double>
                {
                    59d
                };

                using (var session = store.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, "users/ayende");
                    session.TimeSeriesFor("users/ayende", "Heartrate")
                        .Append(baseline.AddMinutes(1), values, "watches/fitbit");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
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
        public void ShouldDeleteTimeSeriesUponDocumentDeletion()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;
                var id = "users/ayende";

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Oren" }, id);

                    var timeSeriesFor = session.TimeSeriesFor(id, "Heartrate");
                    timeSeriesFor.Append(baseline.AddMinutes(1), new []{ 59d }, "watches/fitbit");
                    timeSeriesFor.Append(baseline.AddMinutes(2), new[] { 59d }, "watches/fitbit");

                    session.TimeSeriesFor(id, "Heartrate2")
                        .Append(baseline.AddMinutes(1), new[] { 59d }, "watches/apple");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Delete(id);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var vals = session.TimeSeriesFor(id, "Heartrate").Get(DateTime.MinValue, DateTime.MaxValue);
                    Assert.Null(vals);

                    vals = session.TimeSeriesFor(id, "Heartrate2").Get(DateTime.MinValue, DateTime.MaxValue);
                    Assert.Null(vals);
                }
            }
        }

        [Fact]
        public void CanSkipAndTakeTimeSeries()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, "users/ayende");

                    var tsf = session.TimeSeriesFor("users/ayende", "Heartrate");

                    for (int i = 0; i < 100; i++)
                    {
                        tsf.Append(baseline.AddMinutes(i), new[] { 100d + i }, "watches/fitbit");
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var vals = session.TimeSeriesFor("users/ayende", "Heartrate")
                        .Get(DateTime.MinValue, DateTime.MaxValue, start: 5, pageSize :20)
                        .ToList();

                    Assert.Equal(20, vals.Count);

                    for (int i = 0; i < vals.Count; i++)
                    {
                        Assert.Equal(baseline.AddMinutes(5 + i), vals[i].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                        Assert.Equal(105d + i, vals[i].Value);
                    }
                }
            }
        }

        [Fact]
        public void ShouldEvictTimeSeriesUponEntityEviction()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                var documentId = "users/ayende";

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Oren" }, documentId);

                    var tsf = session.TimeSeriesFor(documentId, "Heartrate");

                    for (int i = 0; i < 60; i++)
                    {
                        tsf.Append(baseline.AddMinutes(i), new[] { 100d + i }, "watches/fitbit");
                    }

                    tsf = session.TimeSeriesFor(documentId, "BloodPressure");

                    for (int i = 0; i < 10; i++)
                    {
                        tsf.Append(baseline.AddMinutes(i), new[] { 120d - i, 80 + i }, "watches/apple");
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>(documentId);

                    var tsf = session.TimeSeriesFor(user, "Heartrate");

                    var vals = tsf.Get(baseline, baseline.AddMinutes(10)).ToList();

                    Assert.Equal(11, vals.Count);

                    vals = tsf.Get(baseline.AddMinutes(20), baseline.AddMinutes(50)).ToList();

                    Assert.Equal(31, vals.Count);

                    tsf = session.TimeSeriesFor(user, "BloodPressure");

                    vals = tsf.Get(DateTime.MinValue, DateTime.MaxValue).ToList();

                    Assert.Equal(10, vals.Count);

                    var sessionOperations = (InMemoryDocumentSessionOperations)session;

                    Assert.Equal(1, sessionOperations.TimeSeriesByDocId.Count);
                    Assert.True(sessionOperations.TimeSeriesByDocId.TryGetValue(documentId, out var cache));
                    Assert.Equal(2, cache.Count);
                    Assert.True(cache.TryGetValue("Heartrate", out var ranges));
                    Assert.Equal(2, ranges.Count);
                    Assert.Equal(baseline, ranges[0].From, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(baseline.AddMinutes(10), ranges[0].To, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(11, ranges[0].Entries.Length);
                    Assert.Equal(baseline.AddMinutes(20), ranges[1].From, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(baseline.AddMinutes(50), ranges[1].To, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(31, ranges[1].Entries.Length);
                    Assert.True(cache.TryGetValue("BloodPressure", out ranges));
                    Assert.Equal(1, ranges.Count);
                    Assert.Equal(DateTime.MinValue, ranges[0].From);
                    Assert.Equal(DateTime.MaxValue, ranges[0].To);
                    Assert.Equal(10, ranges[0].Entries.Length);

                    session.Advanced.Evict(user);

                    Assert.False(sessionOperations.TimeSeriesByDocId.TryGetValue(documentId, out cache));
                    Assert.Equal(0, sessionOperations.TimeSeriesByDocId.Count);

                }
            }
        }

        [Fact]
        public void GetAllTimeSeriesNamesWhenNoTimeSeries()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User(), "users/karmel");
                    session.SaveChanges();
                }
                
                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>("users/karmel");
                    var tsNames = session.Advanced.GetTimeSeriesFor(user);
                    Assert.Equal(0, tsNames.Count);
                }
            }
        }

        [Fact]
        public void GetSingleTimeSeriesWhenNoTimeSeries()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User(), "users/karmel");
                    session.SaveChanges();
                }
                
                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>("users/karmel");
                    var ts = session.TimeSeriesFor(user, "unicorns").Get(DateTime.MinValue, DateTime.MaxValue);
                    Assert.Null(ts);
                }
            }
        }

        [Fact]
        public void CanDeleteWithoutProvidingFromAndToDates()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today.EnsureUtc();

                var docId = "users/ayende";

                using (var session = store.OpenSession())
                {
                    session.Store(new User(), docId);

                    var tsf = session.TimeSeriesFor(docId, "HeartRate");
                    var tsf2 = session.TimeSeriesFor(docId, "BloodPressure");
                    var tsf3 = session.TimeSeriesFor(docId, "BodyTemperature");

                    for (int j = 0; j < 100; j++)
                    {
                        tsf.Append(baseline.AddMinutes(j), j);
                        tsf2.Append(baseline.AddMinutes(j), j);
                        tsf3.Append(baseline.AddMinutes(j), j);
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var tsf = session.TimeSeriesFor(docId, "Heartrate");

                    var entries = tsf.Get()?.ToList();
                    Assert.Equal(100, entries?.Count);

                    // null From, To
                    tsf.Delete();
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var entries = session.TimeSeriesFor(docId, "Heartrate").Get()?.ToList();
                    Assert.Null(entries);
                }

                using (var session = store.OpenSession())
                {
                    var tsf = session.TimeSeriesFor(docId, "BloodPressure");

                    var entries = tsf.Get()?.ToList();
                    Assert.Equal(100, entries?.Count);

                    // null To
                    tsf.Delete(baseline.AddMinutes(50), null);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var entries = session.TimeSeriesFor(docId, "BloodPressure").Get()?.ToList();
                    Assert.Equal(50, entries?.Count);
                }

                using (var session = store.OpenSession())
                {
                    var tsf = session.TimeSeriesFor(docId, "BodyTemperature");

                    var entries = tsf.Get()?.ToList();
                    Assert.Equal(100, entries?.Count);

                    // null From
                    tsf.Delete(null, baseline.AddMinutes(19));
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var entries = session.TimeSeriesFor(docId, "BodyTemperature").Get()?.ToList();
                    Assert.Equal(80, entries?.Count);
                }
            }
        }
    }
}
