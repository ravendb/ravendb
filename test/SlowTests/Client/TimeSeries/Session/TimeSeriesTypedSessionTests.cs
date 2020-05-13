using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Esprima.Ast;
using FastTests;
using Newtonsoft.Json;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Queries.TimeSeries;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Session.TimeSeries;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Tests.Core.Utils.Entities;
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

        public class StockPrice : TimeSeriesEntry
        {
            public double Close { get => Values[0]; set => Values[0] = value; }
            public double High { get => Values[1]; set => Values[1] = value; }
            public double Low { get => Values[2]; set => Values[2] = value; }
            public double Open { get => Values[3]; set => Values[3] = value; }
            public double Volume { get => Values[4]; set => Values[4] = value; }

            public StockPrice()
            {
                Values = new double[5];
            }
        }

        public class HeartRateMeasure : TimeSeriesEntry
        {
            public double HeartRate { get => Values[0]; set => Values[0] = value; }

            public HeartRateMeasure()
            {
                Values = new double[1];
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
                    var measure = new HeartRateMeasure
                    {
                        Timestamp = baseline.AddMinutes(1),
                        HeartRate = 59d,
                        Tag = "watches/fitbit"
                    };
                    session.TimeSeriesFor<HeartRateMeasure>("users/ayende", "Heartrate").Append(measure);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var val = session.TimeSeriesFor<HeartRateMeasure>("users/ayende", "Heartrate")
                        .Get().Single();

                    Assert.Equal(59d , val.HeartRate);
                    Assert.Equal("watches/fitbit", val.Tag);
                    Assert.Equal(baseline.AddMinutes(1), val.Timestamp, RavenTestHelper.DateTimeComparer.Instance);
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
                    var measure = new HeartRateMeasure
                    {
                        Timestamp = baseline.AddMinutes(1),
                        HeartRate = 59d,
                        Tag = "watches/fitbit"
                    };
                    session.TimeSeriesFor<HeartRateMeasure>("users/ayende", "Heartrate").Append(measure);

                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var result = await session.TimeSeriesFor<HeartRateMeasure>("users/ayende", "Heartrate").GetAsync();
                    var val = result.Single();

                    Assert.Equal(59d , val.HeartRate);
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
                    var val = session.TimeSeriesFor<HeartRateMeasure>("users/ayende", "Heartrate")
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
                    var tsf = session.TimeSeriesFor("users/ayende", "Heartrate");
                    tsf.Append(baseline, new[] { 58d }, "watches/fitbit");
                    tsf.Append(baseline.AddMinutes(10), new[] { 60d }, "watches/fitbit");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var vals = session.TimeSeriesFor<HeartRateMeasure>("users/ayende", "Heartrate")
                        .Get(baseline.AddMinutes(-10), baseline.AddMinutes(-5))?
                        .ToList();

                    Assert.Null(vals);

                    vals = session.TimeSeriesFor<HeartRateMeasure>("users/ayende", "Heartrate")
                        .Get(baseline.AddMinutes(5), baseline.AddMinutes(9))?
                        .ToList();

                    Assert.Null(vals);
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
                    session.TimeSeriesFor<HeartRateMeasure>("users/karmel", "Heartrate")
                        .Append(new HeartRateMeasure
                        {
                            HeartRate = 66,
                            Tag = "MyHeart",
                            Timestamp = DateTime.Now
                        });

                    session.TimeSeriesFor<StockPrice>("users/karmel", "Nasdaq")
                        .Append(new StockPrice
                        {
                            Open = 66,
                            Close = 55,
                            High = 113.4,
                            Low = 52.4,
                            Volume = 15472,
                            Timestamp = DateTime.Now
                        });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>("users/karmel");
                    var tsNames = session.Advanced.GetTimeSeriesFor(user);
                    Assert.Equal(2, tsNames.Count);

                    // should be sorted
                    Assert.Equal("Heartrate", tsNames[0]);
                    Assert.Equal("Nasdaq", tsNames[1]);

                    var heartRateMeasures = session.TimeSeriesFor<HeartRateMeasure>(user, "Heartrate").Get().Single();
                    Assert.Equal(66, heartRateMeasures.HeartRate);

                    var stockPrice = session.TimeSeriesFor<StockPrice>(user, "Nasdaq").Get().Single();
                    Assert.Equal(66, stockPrice.Open);
                    Assert.Equal(55, stockPrice.Close);
                    Assert.Equal(113.4, stockPrice.High);
                    Assert.Equal(52.4, stockPrice.Low);
                    Assert.Equal(15472, stockPrice.Volume);
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

                    var tsf = session.TimeSeriesFor<HeartRateMeasure>("users/ayende", "Heartrate");
                    var m = new HeartRateMeasure
                    {
                        Timestamp = baseline.AddMinutes(61), 
                        HeartRate = 59d, 
                        Tag = "watches/fitbit"
                    };
                    tsf.Append(m);

                    m.Timestamp = m.Timestamp.AddMinutes(1);
                    m.HeartRate = 79d;
                    tsf.Append(m);

                    m.Timestamp = m.Timestamp.AddMinutes(1);
                    m.HeartRate = 69d;
                    tsf.Append(m);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesAggregationResult<HeartRateMeasure>>(@"
    declare timeseries out(u) 
    {
        from u.Heartrate between $start and $end
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
                            var reader = tss.GetReader(ctx, "users/ayende", "Heartrate", baseline, baseline.AddDays(1));

                            Assert.True(reader.Init());

                            Assert.NotNull(reader._tvr);

                            var key = reader._tvr.Read(0, out var size);

                            TimeSeriesValuesSegment.ParseTimeSeriesKey(key, size, ctx, out var docId, out var name, out DateTime baseline2);

                            Assert.Equal("users/ayende", docId);
                            Assert.Equal("Heartrate", name);
                            Assert.Equal(baseline.AddMinutes(61), baseline2);

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


                        var tsf = session.TimeSeriesFor<HeartRateMeasure>(id, "Heartrate");
                        var m = new HeartRateMeasure
                        {
                            Timestamp = baseline.AddMinutes(61), 
                            HeartRate = 59d, 
                            Tag = "watches/fitbit"
                        };
                        tsf.Append(m);

                        m.Timestamp = m.Timestamp.AddMinutes(1);
                        m.HeartRate = 79d;
                        tsf.Append(m);

                        m.Timestamp = m.Timestamp.AddMinutes(1);
                        m.HeartRate = 69d;
                        m.Tag = "watches/apple";
                        tsf.Append(m);

                        m.Timestamp = baseline.AddMonths(1).AddMinutes(61);
                        m.HeartRate = 159d;
                        m.Tag = "watches/fitbit";
                        tsf.Append(m);

                        m.Timestamp = m.Timestamp.AddMinutes(1);
                        m.HeartRate = 179d;
                        m.Tag = "watches/apple";
                        tsf.Append(m);

                        m.Timestamp = m.Timestamp.AddMinutes(1);
                        m.HeartRate = 169d;
                        m.Tag = "watches/fitbit";
                        tsf.Append(m);
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesRawResult<HeartRateMeasure>>(@"
declare timeseries out(x) 
{
    from x.HeartRate between $start and $end
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
                        Assert.Equal(59, val.HeartRate);
                        Assert.Equal("watches/fitbit", val.Tag);
                        Assert.Equal(baseline.AddMinutes(61), val.Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                        val = agg.Results[1];

                        Assert.Equal(1, val.Values.Length);
                        Assert.Equal(79, val.HeartRate);
                        Assert.Equal("watches/fitbit", val.Tag);
                        Assert.Equal(baseline.AddMinutes(62), val.Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                        val = agg.Results[2];

                        Assert.Equal(1, val.Values.Length);
                        Assert.Equal(69, val.HeartRate);
                        Assert.Equal("watches/apple", val.Tag);
                        Assert.Equal(baseline.AddMinutes(63), val.Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                        val = agg.Results[3];

                        Assert.Equal(1, val.Values.Length);
                        Assert.Equal(159, val.HeartRate);
                        Assert.Equal("watches/fitbit", val.Tag);
                        Assert.Equal(baseline.AddMonths(1).AddMinutes(61), val.Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                        
                        val = agg.Results[4];

                        Assert.Equal(1, val.Values.Length);
                        Assert.Equal(179, val.HeartRate);
                        Assert.Equal("watches/apple", val.Tag);
                        Assert.Equal(baseline.AddMonths(1).AddMinutes(62), val.Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                        val = agg.Results[5];

                        Assert.Equal(1, val.Values.Length);
                        Assert.Equal(169, val.HeartRate);
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
                        .Select(u => RavenQuery.TimeSeries(u, "Heartrate")
                            .Where(ts => ts.Tag == "watches/fitbit")
                            .GroupBy("1 month")
                            .Select(g => new
                            {
                                Avg = g.Average(),
                                Max = g.Max()
                            }).ToList<HeartRateMeasure>()
                        );

                    var result = query.ToList();

                    Assert.Equal(1, result.Count);
                    Assert.Equal(6, result[0].Count);

                    var agg = result[0].Results;

                    Assert.Equal(2, agg.Length);

                    Assert.Equal(79, agg[0].Max.HeartRate);
                    Assert.Equal(69, agg[0].Average.HeartRate);

                    Assert.Equal(179, agg[1].Max.HeartRate);
                    Assert.Equal(169, agg[1].Average.HeartRate);

                }
            }
        }

         [Fact]
        public void CanQueryTimeSeriesRaw_WhereOnValue()
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
                        .Select(p => RavenQuery.TimeSeries(p, "Heartrate", baseline, baseline.AddMonths(2))
                            .Where(ts => ts.Value > 75 && ts.Value < 175)
                            .ToList<HeartRateMeasure>());

                    var result = query.First();
                    Assert.Equal(3, result.Count);

                    var timeSeriesValues = result.Results;

                    Assert.Equal(79, timeSeriesValues[0].HeartRate);
                    Assert.Equal(baseline.AddMinutes(62), timeSeriesValues[0].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    Assert.Equal(159, timeSeriesValues[1].HeartRate);
                    Assert.Equal(baseline.AddMonths(1).AddMinutes(61), timeSeriesValues[1].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    Assert.Equal(169, timeSeriesValues[2].HeartRate);
                    Assert.Equal(baseline.AddMonths(1).AddMinutes(63), timeSeriesValues[2].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                }
            }
        }
    }
}
