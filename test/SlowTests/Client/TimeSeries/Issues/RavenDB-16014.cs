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
    public class RavenDB_16014 : RavenTestBase
    {
        public RavenDB_16014(ITestOutputHelper output) : base(output)
        {
        }



        [Fact]
        public void CanFillGaps_LastRangeValue_RawQuery()
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
    with interpolation(last)
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

                    // should be the same as last range
                    Assert.Equal(60, aggResult.Max[0]);

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
        public void CanFillGaps_LastRangeValue2_RawQuery()
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
    with interpolation(last)
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
                            // should be the same as last range
                            Assert.Equal((index - 2) * 2.5, tsAggregation.Max[0]);
                            continue;
                        }

                        if (index.In(4, 9))
                        {
                            // should be the same as last range
                            Assert.Equal((index - 3) * 2.5, tsAggregation.Max[0]);
                            continue;
                        }

                        Assert.Equal(index * 2.5, tsAggregation.Max[0]);
                    }
                }
            }
        }

        [Fact]
        public void CanFillGaps_NextRangeValue_RawQuery()
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
    with interpolation(next)
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

                    // should be the same as next range
                    Assert.Equal(90, aggResult.Max[0]);

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
        public void CanFillGaps_NextRangeValue2_RawQuery()
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
    with interpolation(next)
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
                            // should be the same as next range 
                            Assert.Equal((index + 3) * 2.5, tsAggregation.Max[0]);
                            continue;
                        }

                        if (index.In(3, 8))
                        {
                            // should be the same as next range
                            Assert.Equal((index + 2) * 2.5, tsAggregation.Max[0]);
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
        public void CanFillGaps_LastRangeValue_Linq()
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
                                    Interpolation = InterpolationType.Last
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
                            // should be the same as last range
                            Assert.Equal((index - 2) * 2.5, tsAggregation.Max[0]);
                            continue;
                        }

                        if (index.In(4, 9))
                        {
                            // should be the same as last range
                            Assert.Equal((index - 3) * 2.5, tsAggregation.Max[0]);
                            continue;
                        }

                        Assert.Equal(index * 2.5, tsAggregation.Max[0]);
                    }
                }
            }
        }

        [Fact]
        public void CanFillGaps_NextRangeValue_Linq()
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
                                    Interpolation = InterpolationType.Next
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
                            // should be the same as next range 
                            Assert.Equal((index + 3) * 2.5, tsAggregation.Max[0]);
                            continue;
                        }

                        if (index.In(3, 8))
                        {
                            // should be the same as next range
                            Assert.Equal((index + 2) * 2.5, tsAggregation.Max[0]);
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

    }
}
