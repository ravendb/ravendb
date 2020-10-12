using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using FastTests.Client;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Queries.TimeSeries;
using Raven.Client.Documents.Session.TimeSeries;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using SlowTests.Client.TimeSeries.Policies;
using SlowTests.Client.TimeSeries.Query;
using Sparrow;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client.TimeSeries.Session
{
    public class TimeSeriesTypedSessionTests : RavenTestBase
    {
        public TimeSeriesTypedSessionTests(ITestOutputHelper output) : base(output)
        {
        }

        private struct StockPrice
        {
            [TimeSeriesValue(0)] public double Open;
            [TimeSeriesValue(1)] public double Close;
            [TimeSeriesValue(2)] public double High;
            [TimeSeriesValue(3)] public double Low;
            [TimeSeriesValue(4)] public double Volume;
        }

        private class StockPriceWithBadAttributes
        {
#pragma warning disable CS0649
            [TimeSeriesValue(1)] public double Open;
            [TimeSeriesValue(2)] public double Close;
            [TimeSeriesValue(3)] public double High;
            [TimeSeriesValue(4)] public double Low;
            [TimeSeriesValue(5)] public double Volume;
#pragma warning restore CS0649
        }

        internal struct HeartRateMeasure
        {
            [TimeSeriesValue(0)] public double HeartRate;
        }

        [Fact]
        public async Task CanRegisterTimeSeries()
        {
            using (var store = GetDocumentStore())
            {
                await store.TimeSeries.RegisterAsync<User, StockPrice>();
                await store.TimeSeries.RegisterAsync("Users", nameof(HeartRateMeasure) + "s", new[] { nameof(HeartRateMeasure.HeartRate) });

                var updated = (await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database))).TimeSeries;
                var heartrate = updated.GetNames("users", nameof(HeartRateMeasure) + "s");
                Assert.Equal(1, heartrate.Length);
                Assert.Equal(nameof(HeartRateMeasure.HeartRate), heartrate[0]);

                var stock = updated.GetNames("users", nameof(StockPrice) + "s");
                Assert.Equal(5, stock.Length);
                Assert.Equal(nameof(StockPrice.Open), stock[0]);
                Assert.Equal(nameof(StockPrice.Close), stock[1]);
                Assert.Equal(nameof(StockPrice.High), stock[2]);
                Assert.Equal(nameof(StockPrice.Low), stock[3]);
                Assert.Equal(nameof(StockPrice.Volume), stock[4]);
            }
        }

        [Fact]
        public async Task CanRegisterTimeSeriesForOtherDatabase()
        {
            using (var store1 = GetDocumentStore())
            using (var store2 = GetDocumentStore())
            {
                await store1.TimeSeries.ForDatabase(store2.Database).RegisterAsync<User, StockPrice>();
                await store1.TimeSeries.ForDatabase(store2.Database).RegisterAsync("Users", nameof(HeartRateMeasure) + "s", new[] { nameof(HeartRateMeasure.HeartRate) });

                var updated = (await store1.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store2.Database))).TimeSeries;
                Assert.NotNull(updated);
                var heartrate = updated.GetNames("users", nameof(HeartRateMeasure) + "s");
                Assert.Equal(1, heartrate.Length);
                Assert.Equal(nameof(HeartRateMeasure.HeartRate), heartrate[0]);

                var stock = updated.GetNames("users", nameof(StockPrice) + "s");
                Assert.Equal(5, stock.Length);
                Assert.Equal(nameof(StockPrice.Open), stock[0]);
                Assert.Equal(nameof(StockPrice.Close), stock[1]);
                Assert.Equal(nameof(StockPrice.High), stock[2]);
                Assert.Equal(nameof(StockPrice.Low), stock[3]);
                Assert.Equal(nameof(StockPrice.Volume), stock[4]);
            }
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

                    var ts = session.TimeSeriesFor<HeartRateMeasure>("users/ayende");
                    ts.Append(baseline, new HeartRateMeasure { HeartRate = 59d }, "watches/fitbit");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var val = session.TimeSeriesFor<HeartRateMeasure>("users/ayende")
                        .Get().Single();

                    Assert.Equal(59d, val.Value.HeartRate);
                    Assert.Equal("watches/fitbit", val.Tag);
                    Assert.Equal(baseline, val.Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                }
            }
        }

        [Fact]
        public async Task CanCreateSimpleTimeSeriesAsync()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new { Name = "Oren" }, "users/ayende");
                    var measure = new TimeSeriesEntry<HeartRateMeasure>
                    {
                        Timestamp = baseline.AddMinutes(1),
                        Value = new HeartRateMeasure
                        {
                            HeartRate = 59d
                        },
                        Tag = "watches/fitbit"
                    };
                    var ts = session.TimeSeriesFor<HeartRateMeasure>("users/ayende");
                    ts.Append(measure);

                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var result = await session.TimeSeriesFor<HeartRateMeasure>("users/ayende").GetAsync();
                    var val = result.Single();

                    Assert.Equal(59d, val.Value.HeartRate);
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

                    var tsf = session.TimeSeriesFor("users/ayende", nameof(HeartRateMeasure) + "s");

                    tsf.Append(baseline.AddMinutes(1), new[] { 59d }, "watches/fitbit");
                    tsf.Append(baseline.AddMinutes(2), new[] { 60d }, "watches/fitbit");
                    tsf.Append(baseline.AddMinutes(2), new[] { 61d }, "watches/fitbit");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var val = session.TimeSeriesFor<HeartRateMeasure>("users/ayende")
                        .Get()
                        .ToList();
                    Assert.Equal(2, val.Count);
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
                    var tsf = session.TimeSeriesFor("users/ayende", nameof(HeartRateMeasure) + "s");
                    tsf.Append(baseline, new[] { 58d }, "watches/fitbit");
                    tsf.Append(baseline.AddMinutes(10), new[] { 60d }, "watches/fitbit");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var vals = session.TimeSeriesFor<HeartRateMeasure>("users/ayende")
                        .Get(baseline.AddMinutes(-10), baseline.AddMinutes(-5))?
                        .ToList();

                    Assert.Empty(vals);

                    vals = session.TimeSeriesFor<HeartRateMeasure>("users/ayende")
                        .Get(baseline.AddMinutes(5), baseline.AddMinutes(9))?
                        .ToList();

                    Assert.Empty(vals);
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
                    session.TimeSeriesFor<HeartRateMeasure>("users/karmel")
                        .Append(DateTime.Now, new HeartRateMeasure
                        {
                            HeartRate = 66,
                        }, "MyHeart");

                    session.TimeSeriesFor<StockPrice>("users/karmel")
                        .Append(DateTime.Now, new StockPrice
                        {
                            Open = 66,
                            Close = 55,
                            High = 113.4,
                            Low = 52.4,
                            Volume = 15472,
                        });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>("users/karmel");
                    var tsNames = session.Advanced.GetTimeSeriesFor(user);
                    Assert.Equal(2, tsNames.Count);

                    // should be sorted
                    Assert.Equal(nameof(HeartRateMeasure) + "s", tsNames[0]);
                    Assert.Equal(nameof(StockPrice) + "s", tsNames[1]);

                    var heartRateMeasures = session.TimeSeriesFor<HeartRateMeasure>(user).Get().Single();
                    Assert.Equal(66, heartRateMeasures.Value.HeartRate);

                    var (date, value) = session.TimeSeriesFor<StockPrice>(user).Get().Single();
                    Assert.Equal(66, value.Open);
                    Assert.Equal(55, value.Close);
                    Assert.Equal(113.4, value.High);
                    Assert.Equal(52.4, value.Low);
                    Assert.Equal(15472, value.Volume);
                }
            }
        }

        [Fact]
        public unsafe void CanQueryTimeSeriesAggregation_DeclareSyntax_AllDocsQuery()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, "users/ayende");

                    var tsf = session.TimeSeriesFor<HeartRateMeasure>("users/ayende");
                    var tag = "watches/fitbit";
                    var m = new HeartRateMeasure
                    {
                        HeartRate = 59d,
                    };
                    tsf.Append(baseline.AddMinutes(61), m, tag);

                    m.HeartRate = 79d;
                    tsf.Append(baseline.AddMinutes(62), m, tag);

                    m.HeartRate = 69d;
                    tsf.Append(baseline.AddMinutes(63), m, tag);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesAggregationResult<HeartRateMeasure>>(@"
    declare timeseries out(u)
    {
        from u.HeartRateMeasures between $start and $end
        group by 1h
        select min(), max(), first(), last()
    }
    from @all_docs as u
    where id() == 'users/ayende'
    select out(u)
")
                        .AddParameter("start", baseline.EnsureUtc())
                        .AddParameter("end", baseline.AddDays(1).EnsureUtc());

                    var agg = query.First();
                    if (agg.Count != 3)
                    {
                        var db = GetDocumentDatabaseInstanceFor(store).Result;
                        var tss = db.DocumentsStorage.TimeSeriesStorage;
                        using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                        using (ctx.OpenReadTransaction())
                        {
                            var reader = tss.GetReader(ctx, "users/ayende", nameof(HeartRateMeasure) + "s", baseline, baseline.AddDays(1));

                            Assert.True(reader.Init());

                            Assert.NotNull(reader._tvr);

                            var key = reader._tvr.Read(0, out var size);

                            TimeSeriesValuesSegment.ParseTimeSeriesKey(key, size, ctx, out var docId, out var name, out DateTime baseline2);

                            Assert.Equal("users/ayende", docId);
                            Assert.Equal(nameof(HeartRateMeasure) + "s", name, StringComparer.InvariantCultureIgnoreCase);
                            Assert.Equal(baseline.AddMinutes(61), baseline2, RavenTestHelper.DateTimeComparer.Instance);

                            Assert.Equal(1, reader.SegmentsOrValues().Count());

                            Assert.False(query.First().Count == 3, "Query assertion failed once and passed on second try. sanity check passed");

                            //Assert.True(false, "Query assertion failed twice. sanity check passed");
                        }
                    }

                    Assert.Equal(3, agg.Count);

                    Assert.Equal(1, agg.Results.Length);

                    var val = agg.Results[0];
                    Assert.Equal(59, val.First.HeartRate);
                    Assert.Equal(59, val.Min.HeartRate);

                    Assert.Equal(69, val.Last.HeartRate);
                    Assert.Equal(79, val.Max.HeartRate);

                    Assert.Equal(baseline.AddMinutes(60), val.From, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(baseline.AddMinutes(120), val.To, RavenTestHelper.DateTimeComparer.Instance);
                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesAggregation_NoSelectOrGroupBy()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    for (int i = 1; i <= 3; i++)
                    {
                        var id = $"people/{i}";

                        session.Store(new User
                        {
                            Name = "Oren",
                            Age = i * 30
                        }, id);

                        var tsf = session.TimeSeriesFor<HeartRateMeasure>(id);
                        var m = new HeartRateMeasure
                        {
                            HeartRate = 59d,
                        };

                        tsf.Append(baseline.AddMinutes(61), m, "watches/fitbit");

                        m.HeartRate = 79d;
                        tsf.Append(baseline.AddMinutes(62), m, "watches/fitbit");

                        m.HeartRate = 69d;
                        tsf.Append(baseline.AddMinutes(63), m, "watches/apple");

                        m.HeartRate = 159d;
                        tsf.Append(baseline.AddMonths(1).AddMinutes(61), m, "watches/fitbit");

                        m.HeartRate = 179d;
                        tsf.Append(baseline.AddMonths(1).AddMinutes(62), m, "watches/apple");

                        m.HeartRate = 169d;
                        tsf.Append(baseline.AddMonths(1).AddMinutes(63), m, "watches/fitbit");
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesRawResult<HeartRateMeasure>>(@"
declare timeseries out(x)
{
    from x.HeartRateMeasures between $start and $end
}
from Users as doc
where doc.Age > 49
select out(doc)
")
                        .AddParameter("start", baseline.EnsureUtc())
                        .AddParameter("end", baseline.AddMonths(2).EnsureUtc());

                    var result = query.ToList();

                    Assert.Equal(2, result.Count);

                    for (int i = 0; i < 2; i++)
                    {
                        var agg = result[i];

                        Assert.Equal(6, agg.Results.Length);

                        var val = agg.Results[0];

                        Assert.Equal(1, val.Values.Length);
                        Assert.Equal(59, val.Value.HeartRate);
                        Assert.Equal("watches/fitbit", val.Tag);
                        Assert.Equal(baseline.AddMinutes(61), val.Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                        val = agg.Results[1];
                        Assert.Equal(1, val.Values.Length);
                        Assert.Equal(79, val.Value.HeartRate);
                        Assert.Equal("watches/fitbit", val.Tag);
                        Assert.Equal(baseline.AddMinutes(62), val.Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                        val = agg.Results[2];

                        Assert.Equal(1, val.Values.Length);
                        Assert.Equal(69, val.Value.HeartRate);
                        Assert.Equal("watches/apple", val.Tag);
                        Assert.Equal(baseline.AddMinutes(63), val.Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                        val = agg.Results[3];

                        Assert.Equal(1, val.Values.Length);
                        Assert.Equal(159, val.Value.HeartRate);
                        Assert.Equal("watches/fitbit", val.Tag);
                        Assert.Equal(baseline.AddMonths(1).AddMinutes(61), val.Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                        val = agg.Results[4];

                        Assert.Equal(1, val.Values.Length);
                        Assert.Equal(179, val.Value.HeartRate);
                        Assert.Equal("watches/apple", val.Tag);
                        Assert.Equal(baseline.AddMonths(1).AddMinutes(62), val.Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                        val = agg.Results[5];

                        Assert.Equal(1, val.Values.Length);
                        Assert.Equal(169, val.Value.HeartRate);
                        Assert.Equal("watches/fitbit", val.Tag);
                        Assert.Equal(baseline.AddMonths(1).AddMinutes(63), val.Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                    }
                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesAggregation_UsingLinq()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Oren",
                        Age = 35
                    }, "users/ayende");

                    var tsf = session.TimeSeriesFor("users/ayende", "HeartRate");

                    tsf.Append(baseline.AddMinutes(61), new[] { 59d }, "watches/fitbit");
                    tsf.Append(baseline.AddMinutes(62), new[] { 79d }, "watches/fitbit");
                    tsf.Append(baseline.AddMinutes(63), new[] { 69d }, "watches/fitbit");

                    tsf.Append(baseline.AddMonths(1).AddMinutes(61), new[] { 159d }, "watches/fitbit");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(62), new[] { 179d }, "watches/fitbit");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(63), new[] { 169d }, "watches/fitbit");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Query<User>()
                        .Where(u => u.Age > 21)
                        .Select(u => RavenQuery.TimeSeries<HeartRateMeasure>(u, "Heartrate")
                            .Where(ts => ts.Tag == "watches/fitbit")
                            .GroupBy("1 month")
                            .Select(g => new
                            {
                                Avg = g.Average(),
                                Sum = g.Sum(),
                                Max = g.Max()
                            }).ToList()
                        );

                    var result = query.ToList();
                    Assert.Equal(1, result.Count);
                    Assert.Equal(6, result[0].Count);

                    var agg = result[0].Results;
                    Assert.Equal(2, agg.Length);
                    Assert.Equal(79, agg[0].Max.HeartRate);
                    Assert.Equal(207, agg[0].Sum.HeartRate);
                    Assert.Equal(69, agg[0].Average.HeartRate);

                    Assert.Equal(179, agg[1].Max.HeartRate);
                    Assert.Equal(507, agg[1].Sum.HeartRate);
                    Assert.Equal(169, agg[1].Average.HeartRate);
                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesRaw_UsingLinq()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Age = 25
                    }, "people/1");

                    var tsf = session.TimeSeriesFor("people/1", "Heartrate");

                    tsf.Append(baseline.AddMinutes(61), new[] { 59d }, "watches/fitbit");
                    tsf.Append(baseline.AddMinutes(62), new[] { 79d }, "watches/apple");
                    tsf.Append(baseline.AddMinutes(63), new[] { 69d }, "watches/fitbit");

                    tsf.Append(baseline.AddMonths(1).AddMinutes(61), new[] { 159d }, "watches/apple");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(62), new[] { 179d }, "watches/sony");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(63), new[] { 169d }, "watches/fitbit");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Query<User>()
                        .Where(p => p.Age > 21)
                        .Select(p => RavenQuery.TimeSeries<HeartRateMeasure>(p, "Heartrate", baseline, baseline.AddMonths(2))
                            .Where(ts => ts.Values[0] > 75 && ts.Values[0] < 175)
                            .ToList());

                    var result = query.First();
                    Assert.Equal(3, result.Count);

                    var timeSeriesValues = result.Results;

                    Assert.Equal(79, timeSeriesValues[0].Value.HeartRate);
                    Assert.Equal(baseline.AddMinutes(62), timeSeriesValues[0].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    Assert.Equal(159, timeSeriesValues[1].Value.HeartRate);
                    Assert.Equal(baseline.AddMonths(1).AddMinutes(61), timeSeriesValues[1].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    Assert.Equal(169, timeSeriesValues[2].Value.HeartRate);
                    Assert.Equal(baseline.AddMonths(1).AddMinutes(63), timeSeriesValues[2].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                }
            }
        }

        [Fact]
        public async Task CanWorkWithRollupTimeSeries()
        {
            using (var store = GetDocumentStore())
            {
                var raw = new RawTimeSeriesPolicy(TimeSpan.FromHours(24));

                var p1 = new TimeSeriesPolicy("By6Hours", TimeSpan.FromHours(6), raw.RetentionTime * 4);
                var p2 = new TimeSeriesPolicy("By1Day", TimeSpan.FromDays(1), raw.RetentionTime * 5);
                var p3 = new TimeSeriesPolicy("By30Minutes", TimeSpan.FromMinutes(30), raw.RetentionTime * 2);
                var p4 = new TimeSeriesPolicy("By1Hour", TimeSpan.FromMinutes(60), raw.RetentionTime * 3);

                var config = new TimeSeriesConfiguration
                {
                    Collections = new Dictionary<string, TimeSeriesCollectionConfiguration>
                    {
                        ["Users"] = new TimeSeriesCollectionConfiguration
                        {
                            RawPolicy = raw,
                            Policies = new List<TimeSeriesPolicy>
                            {
                                p1,p2,p3,p4
                            }
                        },
                    },
                    PolicyCheckFrequency = TimeSpan.FromSeconds(1)
                };
                await store.Maintenance.SendAsync(new ConfigureTimeSeriesOperation(config));
                await store.TimeSeries.RegisterAsync<User, StockPrice>();

                var database = await GetDocumentDatabaseInstanceFor(store);

                var now = DateTime.UtcNow;
                var nowMinutes = now.Minute;
                now = now.AddMinutes(-nowMinutes);
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(-nowMinutes);

                var baseline = now.AddDays(-12);
                var total = TimeSpan.FromDays(12).TotalMinutes;

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Karmel" }, "users/karmel");
                    var ts = session.TimeSeriesFor<StockPrice>("users/karmel");
                    var entry = new StockPrice();
                    for (int i = 0; i <= total; i++)
                    {
                        entry.Open = i;
                        entry.Close = i + 100_000;
                        entry.High = i + 200_000;
                        entry.Low = i + 300_000;
                        entry.Volume = i + 400_000;
                        ts.Append(baseline.AddMinutes(i), entry, "watches/fitbit");
                    }
                    session.SaveChanges();
                }

                await database.TimeSeriesPolicyRunner.RunRollups();
                await database.TimeSeriesPolicyRunner.DoRetention();

                await QueryFromMultipleTimeSeries.VerifyFullPolicyExecution(store, config.Collections["Users"], rawName: "StockPrices");

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesRawResult<StockPrice>>(@"
declare timeseries out()
{
    from StockPrices
    between $start and $end
}
from Users as u
select out()
")
                        .AddParameter("start", baseline.AddDays(-1))
                        .AddParameter("end", now.AddDays(1));

                    var result = query.Single();
                    var count = result.Results.Length;

                    Assert.Equal(5, result.Results[count - 1440].Values.Length);

                    foreach (var res in result.Results)
                    {
                        Assert.Equal(5, res.Values.Length);
                    }
                }

                using (var session = store.OpenSession())
                {
                    // test the same query using linq

                    var query = session.Query<User>()
                        .Select(u =>
                            RavenQuery.TimeSeries<StockPrice>(u, "StockPrices", baseline.AddDays(-1), now.AddDays(1))
                                .ToList());

                    var result = query.Single();
                    var count = result.Results.Length;

                    Assert.Equal(5, result.Results[count - 1440].Values.Length);

                    foreach (var res in result.Results)
                    {
                        Assert.Equal(5, res.Values.Length);
                    }
                }

                now = DateTime.UtcNow;
                using (var session = store.OpenSession())
                {
                    var ts = session.TimeSeriesRollupFor<StockPrice>("users/karmel", p1.Name);
                    var a = new TimeSeriesRollupEntry<StockPrice>(DateTime.Now)
                    {
                        Max = new StockPrice
                        {
                            Close = 1
                        }
                    };
                    ts.Append(a);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var ts = session.TimeSeriesRollupFor<StockPrice>("users/karmel", p1.Name);
                    var res = ts.Get(from: now.AddMilliseconds(-1)).ToList();
                    Assert.Equal(1, res.Count);
                    Assert.Equal(1, res[0].Max.Close);
                }
            }
        }

        [Fact]
        public async Task CanWorkWithRollupTimeSeries2()
        {
            using (var store = GetDocumentStore())
            {
                var raw = new RawTimeSeriesPolicy(TimeSpan.FromHours(24));

                var p1 = new TimeSeriesPolicy("By6Hours", TimeSpan.FromHours(6), raw.RetentionTime * 4);
                var p2 = new TimeSeriesPolicy("By1Day", TimeSpan.FromDays(1), raw.RetentionTime * 5);
                var p3 = new TimeSeriesPolicy("By30Minutes", TimeSpan.FromMinutes(30), raw.RetentionTime * 2);
                var p4 = new TimeSeriesPolicy("By1Hour", TimeSpan.FromMinutes(60), raw.RetentionTime * 3);

                var config = new TimeSeriesConfiguration
                {
                    Collections = new Dictionary<string, TimeSeriesCollectionConfiguration>
                    {
                        ["Users"] = new TimeSeriesCollectionConfiguration
                        {
                            RawPolicy = raw,
                            Policies = new List<TimeSeriesPolicy>
                            {
                                p1,p2,p3,p4
                            }
                        },
                    },
                    PolicyCheckFrequency = TimeSpan.FromSeconds(1)
                };
                await store.Maintenance.SendAsync(new ConfigureTimeSeriesOperation(config));
                await store.TimeSeries.RegisterAsync<User, StockPrice>();

                var database = await GetDocumentDatabaseInstanceFor(store);

                var now = DateTime.UtcNow;
                var baseline = now.AddDays(-12);
                var total = TimeSpan.FromDays(12).TotalMinutes;

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Karmel" }, "users/karmel");
                    var ts = session.TimeSeriesFor<StockPrice>("users/karmel");
                    var entry = new StockPrice();
                    for (int i = 0; i <= total; i++)
                    {
                        entry.Open = i;
                        entry.Close = i + 100_000;
                        entry.High = i + 200_000;
                        entry.Low = i + 300_000;
                        entry.Volume = i + 400_000;
                        ts.Append(baseline.AddMinutes(i), entry, "watches/fitbit");
                    }
                    session.SaveChanges();
                }

                await database.TimeSeriesPolicyRunner.RunRollups();
                await database.TimeSeriesPolicyRunner.DoRetention();

                await QueryFromMultipleTimeSeries.VerifyFullPolicyExecution(store, config.Collections["Users"], rawName: "StockPrices");

                using (var session = store.OpenSession())
                {
                    var query = session.Query<User>()
                            .Select(u =>
                                RavenQuery.TimeSeries<StockPrice>(u, "StockPrices").Select(x=>
                                    new {
                                       First = x.First(),
                                       Last = x.Last(),
                                       Min = x.Min(),
                                       Max = x.Max(),
                                       Sum = x.Sum(),
                                       Count = x.Count(),
                                       Avg = x.Average()
                                    })
                                    .ToList());

                    var result = query.Single();

                    Assert.Equal(1, result.Results.Length);
                    var r = result.Results[0];
                    Assert.NotNull(r.First);
                    Assert.NotNull(r.Last);
                    Assert.NotNull(r.Min);
                    Assert.NotNull(r.Max);
                    Assert.NotNull(r.Sum);
                    Assert.NotNull(r.Count);
                    Assert.NotNull(r.Average);
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Query<User>()
                        .Select(u =>
                            RavenQuery.TimeSeries<StockPrice>(u, "StockPrices")
                                .GroupBy(x => x.Hours(3))
                                .Select(x =>
                                    new
                                    {
                                        First = x.First(),
                                        Last = x.Last(),
                                        Min = x.Min(),
                                        Max = x.Max(),
                                        Sum = x.Sum(),
                                        Count = x.Count(),
                                        Avg = x.Average()
                                    })
                                .ToList());

                    var result = query.Single();
                    var r = result.Results[0];
                    Assert.NotNull(r.First);
                    Assert.NotNull(r.Last);
                    Assert.NotNull(r.Min);
                    Assert.NotNull(r.Max);
                    Assert.NotNull(r.Sum);
                    Assert.NotNull(r.Count);
                    Assert.NotNull(r.Average);
                }

                using (var session = store.OpenSession())
                {
                    var ts1 = session.TimeSeriesRollupFor<StockPrice>("users/karmel", p1.Name);
                    var r = ts1.Get().First();
                    Assert.NotNull(r.First);
                    Assert.NotNull(r.Last);
                    Assert.NotNull(r.Min);
                    Assert.NotNull(r.Max);
                    Assert.NotNull(r.Sum);
                    Assert.NotNull(r.Count);
                    Assert.NotNull(r.Average);
                }
            }
        }

        [Fact]
        public void UsingDifferentNumberOfValues_LargeToSmall()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today.AddDays(-1);

                using (var session = store.OpenSession())
                {
                    session.Store(new User {Name = "Karmel"}, "users/karmel");
                    var big = session.TimeSeriesFor<TimeSeriesConfigurationTests.BigMeasure>("users/karmel");
                    for (int i = 0; i < 5; i++)
                    {
                            big.Append(baseline.AddSeconds(3 * i), new TimeSeriesConfigurationTests.BigMeasure
                            {
                                Measure1 = i,
                                Measure2 = i,
                                Measure3 = i,
                                Measure4 = i,
                                Measure5 = i,
                                Measure6 = i,
                            }, "watches/fitbit");
                    }
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var big = session.TimeSeriesFor("users/karmel", "BigMeasures");
                    for (int i = 5; i < 10; i++)
                    {
                        big.Append(baseline.AddHours(12).AddSeconds(3 * i), i , "watches/fitbit");
                    }
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var big = session.TimeSeriesFor<TimeSeriesConfigurationTests.BigMeasure>("users/karmel").Get()?.ToList();
                    for (int i = 0; i < 5; i++)
                    {
                        var m = big[i].Value;
                        Assert.Equal(i, m.Measure1);
                        Assert.Equal(i, m.Measure2);
                        Assert.Equal(i, m.Measure3);
                        Assert.Equal(i, m.Measure4);
                        Assert.Equal(i, m.Measure5);
                        Assert.Equal(i, m.Measure6);
                    }

                    for (int i = 5; i < 10; i++)
                    {
                        var m = big[i].Value;
                        Assert.Equal(i, m.Measure1);
                        Assert.Equal(double.NaN, m.Measure2);
                        Assert.Equal(double.NaN, m.Measure3);
                        Assert.Equal(double.NaN, m.Measure4);
                        Assert.Equal(double.NaN, m.Measure5);
                        Assert.Equal(double.NaN, m.Measure6);
                    }
                }
            }
        }

        [Fact]
        public void MappingNeedsToContainConsecutiveValuesStartingFromZero()
        {
            using (var store = GetDocumentStore())
            {
                var e = Assert.Throws<InvalidOperationException>(() => store.TimeSeries.Register<Company, StockPriceWithBadAttributes>());
                Assert.Contains("must contain consecutive values starting from 0.", e.Message);
            }
        }
    }
}
