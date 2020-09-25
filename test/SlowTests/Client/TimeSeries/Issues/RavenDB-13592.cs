using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Queries.TimeSeries;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client.TimeSeries.Issues
{
    public class RavenDB_13592 : RavenTestBase
    {
        public RavenDB_13592(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanFillGaps_LinearInterpolation_RawQuery()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;
                string id = "people/1";

                using (var session = store.OpenSession())
                {
                    session.Store(new Person(), id);

                    var tsf = session.TimeSeriesFor(id, "HeartRate");

                    tsf.Append(baseline, 50); // 00:00 - 01:00
                    tsf.Append(baseline.AddHours(1), 60); // 01:00 - 02:00
                    tsf.Append(baseline.AddHours(4), 90); // 04 : 00 - 05:00
                    tsf.Append(baseline.AddHours(5), 100); // 05 : 00 - 06:00

                    // gaps to fill : 02:00 - 03:00, 03:00 - 04:00

                    session.SaveChanges();
                }
                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesAggregationResult>(@"
from People
where id() = $id
select timeseries(
    from HeartRate between $start and $end
    group by 1h
    with interpolation(linear)
    select max())
")
                        .AddParameter("id", id)
                        .AddParameter("start", baseline)
                        .AddParameter("end", baseline.AddDays(1));

                    var result = query.First();

                    Assert.Equal(6, result.Results.Length);

                    var aggResult = result.Results[0];
                    Assert.Equal(baseline, aggResult.From);
                    Assert.Equal(baseline.AddHours(1), aggResult.To);
                    Assert.Equal(50, aggResult.Max[0]);

                    aggResult = result.Results[1];
                    Assert.Equal(baseline.AddHours(1), aggResult.From);
                    Assert.Equal(baseline.AddHours(2), aggResult.To);
                    Assert.Equal(60, aggResult.Max[0]);

                    aggResult = result.Results[2];
                    Assert.Equal(baseline.AddHours(2), aggResult.From);
                    Assert.Equal(baseline.AddHours(3), aggResult.To);
                    Assert.Equal(70, aggResult.Max[0]);

                    aggResult = result.Results[3];
                    Assert.Equal(baseline.AddHours(3), aggResult.From);
                    Assert.Equal(baseline.AddHours(4), aggResult.To);
                    Assert.Equal(80, aggResult.Max[0]);

                    aggResult = result.Results[4];
                    Assert.Equal(baseline.AddHours(4), aggResult.From);
                    Assert.Equal(baseline.AddHours(5), aggResult.To);
                    Assert.Equal(90, aggResult.Max[0]);

                    aggResult = result.Results[5];
                    Assert.Equal(baseline.AddHours(5), aggResult.From);
                    Assert.Equal(baseline.AddHours(6), aggResult.To);
                    Assert.Equal(100, aggResult.Max[0]);
                }
            }
        }

        [Fact]
        public void CanFillGaps_LinearInterpolation2_RawQuery()
        {
            using (var store = GetDocumentStore())
            {
                var id = "people/1";
                var baseline = RavenTestHelper.UtcToday;
                var totalMinutes = TimeSpan.FromDays(1).TotalMinutes;

                using (var session = store.OpenSession())
                {
                    session.Store(new Person(), id);

                    var tsf = session.TimeSeriesFor(id, "HeartRate");

                    tsf.Append(baseline, 0);
                    for (int i = 1; i <= totalMinutes; i++)
                    {
                        if (i % 3 == 0)
                            continue;
                        tsf.Append(baseline.AddMinutes(i), i * 10);
                    }

                    session.SaveChanges();
                }
                using (var session = store.OpenSession())
                {
                    var get = session.TimeSeriesFor(id, "HeartRate").Get();
                    var entriesCount = get.Length;
                    Assert.Equal(961, entriesCount);

                    var query = session.Advanced.RawQuery<TimeSeriesAggregationResult>(@"
from People
where id() = $id
select timeseries(
    from HeartRate between $start and $end
    group by 1 minute
    with interpolation(linear)
    select max())
")
                        .AddParameter("id", id)
                        .AddParameter("start", baseline)
                        .AddParameter("end", baseline.AddDays(1));

                    var result = query.First();

                    Assert.Equal(totalMinutes, result.Results.Length);
                    Assert.Equal(totalMinutes, result.Count);

                    for (var i = 0; i < result.Results.Length; i++)
                    {
                        var tsAggregation = result.Results[i];
                        Assert.Equal(baseline.AddMinutes(i), tsAggregation.From);
                        Assert.Equal(baseline.AddMinutes(i + 1), tsAggregation.To);
                        Assert.Equal(i * 10, tsAggregation.Max[0]);
                    }
                }
            }
        }

        [Fact]
        public void CanFillGaps_NearestNeighbor_RawQuery()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;
                string id = "people/1";

                using (var session = store.OpenSession())
                {
                    session.Store(new Person(), id);

                    var tsf = session.TimeSeriesFor(id, "HeartRate");

                    tsf.Append(baseline, 50); // 00:00 - 01:00
                    tsf.Append(baseline.AddHours(1), 60); // 01:00 - 02:00
                    tsf.Append(baseline.AddHours(4), 90); // 04 : 00 - 05:00
                    tsf.Append(baseline.AddHours(5), 100); // 05 : 00 - 06:00

                    // gaps to fill : 02:00 - 03:00, 03:00 - 04:00

                    session.SaveChanges();
                }
                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesAggregationResult>(@"
from People
where id() = $id
select timeseries(
    from HeartRate between $start and $end
    group by 1h
    with interpolation(nearest)
    select max())
")
                        .AddParameter("id", id)
                        .AddParameter("start", baseline)
                        .AddParameter("end", baseline.AddDays(1));

                    var result = query.First();

                    Assert.Equal(6, result.Results.Length);

                    var aggResult = result.Results[0];
                    Assert.Equal(baseline, aggResult.From);
                    Assert.Equal(baseline.AddHours(1), aggResult.To);
                    Assert.Equal(50, aggResult.Max[0]);

                    aggResult = result.Results[1];
                    Assert.Equal(baseline.AddHours(1), aggResult.From);
                    Assert.Equal(baseline.AddHours(2), aggResult.To);
                    Assert.Equal(60, aggResult.Max[0]);

                    aggResult = result.Results[2];
                    Assert.Equal(baseline.AddHours(2), aggResult.From);
                    Assert.Equal(baseline.AddHours(3), aggResult.To);

                    // should be the same as last range
                    Assert.Equal(60, aggResult.Max[0]);

                    aggResult = result.Results[3];
                    Assert.Equal(baseline.AddHours(3), aggResult.From);
                    Assert.Equal(baseline.AddHours(4), aggResult.To);

                    // should be the same as next range
                    Assert.Equal(90, aggResult.Max[0]);

                    aggResult = result.Results[4];
                    Assert.Equal(baseline.AddHours(4), aggResult.From);
                    Assert.Equal(baseline.AddHours(5), aggResult.To);
                    Assert.Equal(90, aggResult.Max[0]);

                    aggResult = result.Results[5];
                    Assert.Equal(baseline.AddHours(5), aggResult.From);
                    Assert.Equal(baseline.AddHours(6), aggResult.To);
                    Assert.Equal(100, aggResult.Max[0]);
                }
            }
        }

        [Fact]
        public void CanFillGaps_NearestNeighbor2_RawQuery()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;
                string id = "people/1";

                using (var session = store.OpenSession())
                {
                    session.Store(new Person(), id);

                    var tsf = session.TimeSeriesFor(id, "HeartRate");
                    tsf.Append(baseline, 0);

                    for (int i = 1; i <= 10; i++)
                    {
                        if (i.In(2, 3, 4, 7, 8, 9))
                            continue;

                        tsf.Append(baseline.AddMinutes(i), i * 2.5);
                    }

                    session.SaveChanges();
                }
                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesAggregationResult>(@"
from People
where id() = $id
select timeseries(
    from HeartRate between $start and $end
    group by 1 minute
    with interpolation(nearest)
    select max())
")
                        .AddParameter("id", id)
                        .AddParameter("start", baseline)
                        .AddParameter("end", baseline.AddDays(1));

                    var result = query.First();

                    Assert.Equal(11, result.Results.Length);

                    for (var index = 0; index < result.Results.Length; index++)
                    {
                        var tsAggregation = result.Results[index];
                        Assert.Equal(baseline.AddMinutes(index), tsAggregation.From);
                        Assert.Equal(baseline.AddMinutes(index + 1), tsAggregation.To);

                        if (index.In(2, 7))
                        {
                            // should be the same as last range
                            Assert.Equal((index - 1) * 2.5, tsAggregation.Max[0]);
                            continue;
                        }

                        if (index.In(3, 8))
                        {
                            // should be the same as last (real) range
                            Assert.Equal((index - 2) * 2.5, tsAggregation.Max[0]);
                            continue;
                        }

                        if (index.In(4, 9))
                        {
                            // should be the same as next range
                            Assert.Equal((index + 1) * 2.5, tsAggregation.Max[0]);
                            continue;
                        }

                        Assert.Equal(index * 2.5, tsAggregation.Max[0]);
                    }
                }
            }
        }

        [Fact]
        public void CanFillGaps_GroupByMonth_Linear_RawQuery()
        {
            using (var store = GetDocumentStore())
            {
                var id = "people/1";
                var baseline = new DateTime(2020, 4, 1);

                using (var session = store.OpenSession())
                {
                    session.Store(new Person(), id);

                    var tsf = session.TimeSeriesFor(id, "HeartRate");
                    tsf.Append(baseline, 0);

                    var dt = baseline;
                    var c = 0;

                    while (dt < baseline.AddYears(2))
                    {
                        dt = dt.AddMonths(1);
                        c++;

                        if (dt.Month.In(10, 11, 12))
                            continue;

                        tsf.Append(dt, c * 10);
                    }

                    session.SaveChanges();
                }
                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesAggregationResult>(@"
from People
where id() = $id
select timeseries(
    from HeartRate between $start and $end
    group by 1 month
    with interpolation(linear)
    select max())
")
                        .AddParameter("id", id)
                        .AddParameter("start", baseline)
                        .AddParameter("end", baseline.AddYears(2));

                    var result = query.First();

                    Assert.Equal(25, result.Results.Length);

                    for (var i = 0; i < result.Results.Length; i++)
                    {
                        var tsAggregation = result.Results[i];
                        Assert.Equal(baseline.AddMonths(i), tsAggregation.From);
                        Assert.Equal(baseline.AddMonths(i + 1), tsAggregation.To);

                        if (tsAggregation.From.Month.In(10, 11, 12))
                        {
                            // y = yA + (yB - yA) * ((x - xa) / (xb - xa))
                            var x = tsAggregation.From.Ticks;
                            var xa = result.Results[i - 1].From.Ticks;
                            var xb = result.Results[i + 1].From.Ticks;
                            var ya = result.Results[i - 1].Max[0];
                            var yb = result.Results[i + 1].Max[0];

                            var expected = ya + (yb - ya) * ((double)(x - xa) / (xb - xa));

                            Assert.Equal(expected, tsAggregation.Max[0]);
                            continue;
                        }

                        Assert.Equal(i * 10, tsAggregation.Max[0]);
                    }
                }
            }
        }

        [Fact]
        public void CanFillGaps_GroupByMonth_Nearest_RawQuery()
        {
            using (var store = GetDocumentStore())
            {
                var id = "people/1";
                var dt1 = new DateTime(2020, 2, 1);
                var dt2 = new DateTime(2020, 3, 30);
                var dt3 = new DateTime(2020, 5, 1);

                using (var session = store.OpenSession())
                {
                    session.Store(new Person(), id);

                    var tsf = session.TimeSeriesFor(id, "HeartRate");
                    tsf.Append(dt1, 100);
                    tsf.Append(dt2, 200);
                    tsf.Append(dt3, 400);

                    session.SaveChanges();
                }
                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesAggregationResult>(@"
from People
where id() = $id
select timeseries(
    from HeartRate between $start and $end
    group by 1 month
    with interpolation(nearest)
    select max())
")
                        .AddParameter("id", id)
                        .AddParameter("start", dt1.AddYears(-1))
                        .AddParameter("end", dt1.AddYears(1));

                    var result = query.First();

                    Assert.Equal(4, result.Results.Length);

                    var tsAggregation = result.Results[0];
                    Assert.Equal(100, tsAggregation.Max[0]);

                    tsAggregation = result.Results[1];
                    Assert.Equal(200, tsAggregation.Max[0]);

                    tsAggregation = result.Results[2];

                    //should be same as next
                    Assert.Equal(400, tsAggregation.Max[0]);

                    tsAggregation = result.Results[3];
                    Assert.Equal(400, tsAggregation.Max[0]);
                }
            }
        }

        [Fact]
        public void CanFillGaps_MultipleValues_RawQuery()
        {
            using (var store = GetDocumentStore())
            {
                var id = "people/1";
                var baseline = RavenTestHelper.UtcToday;
                var totalMinutes = TimeSpan.FromDays(1).TotalMinutes;

                using (var session = store.OpenSession())
                {
                    session.Store(new Person(), id);

                    var tsf = session.TimeSeriesFor(id, "HeartRate");

                    tsf.Append(baseline, new[] { 0d, 0 });

                    for (int i = 1; i <= totalMinutes; i++)
                    {
                        if (i % 3 == 0)
                            continue;
                        tsf.Append(baseline.AddMinutes(i), new[] { i * 3d, i * 6.5 });
                    }

                    session.SaveChanges();
                }
                using (var session = store.OpenSession())
                {
                    var get = session.TimeSeriesFor(id, "HeartRate").Get();
                    var entriesCount = get.Length;
                    Assert.Equal(961, entriesCount);

                    var query = session.Advanced.RawQuery<TimeSeriesAggregationResult>(@"
from People
where id() = $id
select timeseries(
    from HeartRate between $start and $end
    group by 1 minute
    with interpolation(linear)
    select max())
")
                        .AddParameter("id", id)
                        .AddParameter("start", baseline)
                        .AddParameter("end", baseline.AddDays(1));

                    var result = query.First();

                    Assert.Equal(totalMinutes, result.Results.Length);
                    Assert.Equal(totalMinutes, result.Count);

                    for (var i = 0; i < result.Results.Length; i++)
                    {
                        var tsAggregation = result.Results[i];
                        Assert.Equal(baseline.AddMinutes(i), tsAggregation.From);
                        Assert.Equal(baseline.AddMinutes(i + 1), tsAggregation.To);
                        Assert.Equal(i * 3, tsAggregation.Max[0]);
                        Assert.Equal(i * 6.5, tsAggregation.Max[1]);
                    }
                }
            }
        }

        [Fact]
        public void CanFillGaps_DifferentNumberOfValues_RawQuery()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;
                string id = "people/1";

                using (var session = store.OpenSession())
                {
                    session.Store(new Person(), id);

                    var tsf = session.TimeSeriesFor(id, "HeartRate");

                    tsf.Append(baseline, 0);
                    tsf.Append(baseline.AddDays(1), 1);
                    tsf.Append(baseline.AddDays(30), new[] { 30d, 60 });
                    tsf.Append(baseline.AddDays(60), new[] { 60d, 120, 180 });
                    tsf.Append(baseline.AddDays(90), 90);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesAggregationResult>(@"
from People
where id() = $id
select timeseries(
    from HeartRate between $start and $end
    group by 1 day
    with interpolation(linear)
    select max())
")
                        .AddParameter("id", id)
                        .AddParameter("start", baseline)
                        .AddParameter("end", baseline.AddYears(1));

                    var result = query.First();

                    Assert.Equal(91, result.Results.Length);

                    for (int i = 0; i < 30; i++)
                    {
                        var tsAgg = result.Results[i];
                        Assert.Equal(1, tsAgg.Max.Length);
                    }

                    for (int i = 30; i < 60; i++)
                    {
                        var tsAgg = result.Results[i];
                        Assert.Equal(2, tsAgg.Max.Length);
                    }

                    Assert.Equal(3, result.Results[60].Max.Length);

                    for (int i = 61; i < result.Results.Length; i++)
                    {
                        var tsAgg = result.Results[i];
                        Assert.Equal(1, tsAgg.Max.Length);
                    }
                }
            }
        }

        [Fact]
        public void CanFillGaps_LinearInterpolation_Linq()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;
                string id = "people/1";

                using (var session = store.OpenSession())
                {
                    session.Store(new Person(), id);

                    var tsf = session.TimeSeriesFor(id, "HeartRate");

                    tsf.Append(baseline, 50);
                    tsf.Append(baseline.AddHours(1), 60);
                    tsf.Append(baseline.AddHours(4), 90);
                    tsf.Append(baseline.AddHours(5), 100);

                    session.SaveChanges();
                }
                using (var session = store.OpenSession())
                {
                    var query = session.Query<Person>()
                        .Where(p => p.Id == id)
                        .Select(p => RavenQuery.TimeSeries(p, "HeartRate", baseline, baseline.AddDays(1))
                            .GroupBy(g => g
                                .Hours(1)
                                .WithOptions(new TimeSeriesAggregationOptions
                                {
                                    Interpolation = InterpolationType.Linear
                                }))
                            .Select(x => x.Max())
                            .ToList());

                    var result = query.First();

                    Assert.Equal(6, result.Results.Length);

                    var aggResult = result.Results[0];
                    Assert.Equal(baseline, aggResult.From);
                    Assert.Equal(baseline.AddHours(1), aggResult.To);
                    Assert.Equal(50, aggResult.Max[0]);

                    aggResult = result.Results[1];
                    Assert.Equal(baseline.AddHours(1), aggResult.From);
                    Assert.Equal(baseline.AddHours(2), aggResult.To);
                    Assert.Equal(60, aggResult.Max[0]);

                    aggResult = result.Results[2];
                    Assert.Equal(baseline.AddHours(2), aggResult.From);
                    Assert.Equal(baseline.AddHours(3), aggResult.To);
                    Assert.Equal(70, aggResult.Max[0]);

                    aggResult = result.Results[3];
                    Assert.Equal(baseline.AddHours(3), aggResult.From);
                    Assert.Equal(baseline.AddHours(4), aggResult.To);
                    Assert.Equal(80, aggResult.Max[0]);

                    aggResult = result.Results[4];
                    Assert.Equal(baseline.AddHours(4), aggResult.From);
                    Assert.Equal(baseline.AddHours(5), aggResult.To);
                    Assert.Equal(90, aggResult.Max[0]);

                    aggResult = result.Results[5];
                    Assert.Equal(baseline.AddHours(5), aggResult.From);
                    Assert.Equal(baseline.AddHours(6), aggResult.To);
                    Assert.Equal(100, aggResult.Max[0]);
                }
            }
        }

        [Fact]
        public void CanFillGaps_NearestNeighbor_Linq()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;
                string id = "people/1";

                using (var session = store.OpenSession())
                {
                    session.Store(new Person(), id);

                    var tsf = session.TimeSeriesFor(id, "HeartRate");
                    tsf.Append(baseline, 0);

                    for (int i = 1; i <= 10; i++)
                    {
                        if (i.In(2, 3, 4, 7, 8, 9))
                            continue;

                        tsf.Append(baseline.AddMinutes(i), i * 2.5);
                    }

                    session.SaveChanges();
                }
                using (var session = store.OpenSession())
                {
                    var query = session.Query<Person>()
                        .Where(p => p.Id == id)
                        .Select(p => RavenQuery.TimeSeries(p, "HeartRate", baseline, baseline.AddDays(1))
                            .GroupBy(g => g
                                .Minutes(1)
                                .WithOptions(new TimeSeriesAggregationOptions
                                {
                                    Interpolation = InterpolationType.Nearest
                                }))
                            .Select(x => x.Max())
                            .ToList());

                    var result = query.First();

                    Assert.Equal(11, result.Results.Length);

                    for (var index = 0; index < result.Results.Length; index++)
                    {
                        var tsAggregation = result.Results[index];
                        Assert.Equal(baseline.AddMinutes(index), tsAggregation.From);
                        Assert.Equal(baseline.AddMinutes(index + 1), tsAggregation.To);

                        if (index.In(2, 7))
                        {
                            // should be the same as last range
                            Assert.Equal((index - 1) * 2.5, tsAggregation.Max[0]);
                            continue;
                        }

                        if (index.In(3, 8))
                        {
                            // should be the same as last (real) range
                            Assert.Equal((index - 2) * 2.5, tsAggregation.Max[0]);
                            continue;
                        }

                        if (index.In(4, 9))
                        {
                            // should be the same as next range
                            Assert.Equal((index + 1) * 2.5, tsAggregation.Max[0]);
                            continue;
                        }

                        Assert.Equal(index * 2.5, tsAggregation.Max[0]);
                    }
                }
            }
        }

        [Fact]
        public void CanFillGaps_DifferentNumberOfValues_Linq()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;
                string id = "people/1";

                using (var session = store.OpenSession())
                {
                    session.Store(new Person(), id);

                    var tsf = session.TimeSeriesFor(id, "HeartRate");

                    tsf.Append(baseline, 0);
                    tsf.Append(baseline.AddDays(1), 1);
                    tsf.Append(baseline.AddDays(30), new[] { 30d, 60 });
                    tsf.Append(baseline.AddDays(60), new[] { 60d, 120, 180 });
                    tsf.Append(baseline.AddDays(90), 90);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Query<Person>()
                        .Where(p => p.Id == id)
                        .Select(p => RavenQuery.TimeSeries(p, "HeartRate", baseline, baseline.AddYears(1))
                            .GroupBy(g => g
                                .Days(1)
                                .WithOptions(new TimeSeriesAggregationOptions
                                {
                                    Interpolation = InterpolationType.Linear
                                }))
                            .Select(x => x.Max())
                            .ToList());

                    var result = query.First();

                    Assert.Equal(91, result.Results.Length);

                    for (int i = 0; i < 30; i++)
                    {
                        var tsAgg = result.Results[i];
                        Assert.Equal(1, tsAgg.Max.Length);
                    }

                    for (int i = 30; i < 60; i++)
                    {
                        var tsAgg = result.Results[i];
                        Assert.Equal(2, tsAgg.Max.Length);
                    }

                    Assert.Equal(3, result.Results[60].Max.Length);

                    for (int i = 61; i < result.Results.Length; i++)
                    {
                        var tsAgg = result.Results[i];
                        Assert.Equal(1, tsAgg.Max.Length);
                    }
                }
            }
        }

        [Fact]
        public void CanFillGaps_SeveralValuesPerBucket_Linq()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;
                string id = "people/1";

                using (var session = store.OpenSession())
                {
                    session.Store(new Person(), id);

                    var tsf = session.TimeSeriesFor(id, "HeartRate");

                    tsf.Append(baseline, 0);
                    tsf.Append(baseline.AddMinutes(10), 1);
                    tsf.Append(baseline.AddMinutes(30), 3);
                    tsf.Append(baseline.AddMinutes(50), 5);
                    tsf.Append(baseline.AddMinutes(60), 6);
                    tsf.Append(baseline.AddMinutes(90), 9);
                    tsf.Append(baseline.AddMinutes(110), 11);
                    tsf.Append(baseline.AddMinutes(190), 19);
                    tsf.Append(baseline.AddMinutes(230), 23);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Query<Person>()
                        .Where(p => p.Id == id)
                        .Select(p => RavenQuery.TimeSeries(p, "HeartRate", baseline, baseline.AddDays(1))
                            .GroupBy(g => g
                                .Hours(1)
                                .WithOptions(new TimeSeriesAggregationOptions
                                {
                                    Interpolation = InterpolationType.Linear
                                }))
                            .Select(x => x.Max())
                            .ToList());

                    var result = query.First();

                    Assert.Equal(4, result.Results.Length);

                    var tsAgg = result.Results[0];
                    Assert.Equal(baseline, tsAgg.From);
                    Assert.Equal(baseline.AddHours(1), tsAgg.To);
                    Assert.Equal(5, tsAgg.Max[0]);

                    tsAgg = result.Results[1];
                    Assert.Equal(baseline.AddHours(1), tsAgg.From);
                    Assert.Equal(baseline.AddHours(2), tsAgg.To);
                    Assert.Equal(11, tsAgg.Max[0]);

                    tsAgg = result.Results[2];
                    Assert.Equal(baseline.AddHours(2), tsAgg.From);
                    Assert.Equal(baseline.AddHours(3), tsAgg.To);

                    var expected = (11 + 23) / 2;
                    Assert.Equal(expected, tsAgg.Max[0]);

                    tsAgg = result.Results[3];
                    Assert.Equal(baseline.AddHours(3), tsAgg.From);
                    Assert.Equal(baseline.AddHours(4), tsAgg.To);
                    Assert.Equal(23, tsAgg.Max[0]);
                }
            }
        }
    }
}
