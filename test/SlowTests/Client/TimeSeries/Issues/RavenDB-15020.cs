using System;
using System.Collections.Generic;
using System.Linq;
using FastTests.Server.Replication;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Queries.TimeSeries;
using Raven.Tests.Core.Utils.Entities;
using Sparrow;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client.TimeSeries.Issues
{
    public class RavenDB_15020 : ReplicationTestBase
    {
        public RavenDB_15020(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanUsePercentileInTimeSeriesQuery_RawQuery()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today.EnsureUtc();
                var id = "people/1";

                var values = new List<double>
                {
                    43, 54, 56, 61, 62,
                    66, 68, 69, 69, 70,
                    71, 72, 77, 78, 79,
                    85, 87, 88, 89, 93,
                    95, 96, 98, 99, 99
                };

                using (var session = store.OpenSession())
                {
                    session.Store(new Person
                    {
                        Name = "Oren",
                    }, id);

                    var tsf = session.TimeSeriesFor(id, "HeartRate");

                    var count = values.Count;
                    var rand = new Random();

                    for (int i = 0; i < count; i++)
                    {
                        var index = rand.Next(0, values.Count - 1);

                        tsf.Append(baseline.AddHours(i), values[index]);

                        values.RemoveAt(index);
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    const double number = 90;
                    var query = session.Advanced.RawQuery<TimeSeriesAggregationResult>(@"
from People 
select timeseries(
    from HeartRate 
    select percentile($n)
)")
                        .AddParameter("n", number);

                    var result = query.First();
                    Assert.Equal(1, result.Results.Length);

                    Assert.Equal(98, result.Results[0].Percentile[0]);
                }
            }
        }

        [Fact]
        public void CanUsePercentileInTimeSeriesQuery_WithGroupBy_RawQuery()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today.EnsureUtc();
                var id = "people/1";

                using (var session = store.OpenSession())
                {
                    session.Store(new Person
                    {
                        Name = "Oren",
                    }, id);

                    var tsf = session.TimeSeriesFor(id, "HeartRate");
                    var rand = new Random();

                    for (int i = 0; i < TimeSpan.FromDays(1).TotalMinutes; i++)
                    {
                        if (i % 7 == 0)
                            continue;

                        var value = rand.NextDouble();

                        tsf.Append(baseline.AddMinutes(i), value);
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var number = new Random().NextDouble() * 100;

                    var allEntries = session.TimeSeriesFor(id, "HeartRate").Get();
                    var groupByHour = allEntries
                        .GroupBy(e => new
                        {
                            e.Timestamp.Day,
                            e.Timestamp.Hour
                        })
                        .ToList();

                    var query = session.Advanced.RawQuery<TimeSeriesAggregationResult>(@"
from People 
select timeseries(
    from HeartRate 
    group by 1h
    select percentile($n)
)")
                        .AddParameter("n", number);

                    var result = query.First();

                    Assert.Equal(24, result.Results.Length);

                    foreach (var rangeAggregation in result.Results)
                    {
                        var day = rangeAggregation.From.Day;
                        var hour = rangeAggregation.From.Hour;

                        var group = groupByHour.SingleOrDefault(x => x.Key.Day == day && x.Key.Hour == hour);
                        Assert.NotNull(group);

                        var groupValues = group.Select(g => g.Value).ToList();
                        groupValues.Sort();

                        var index = (int)Math.Ceiling((number / 100) * groupValues.Count);
                        var expected = groupValues[index - 1];

                        Assert.Equal(expected, rangeAggregation.Percentile[0]);
                    }
                }
            }
        }

        [Fact]
        public void CanUsePercentileInTimeSeriesQuery_WithWhere_RawQuery()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today.EnsureUtc();
                var id = "people/1";

                var tags = new[] { "watches/fitbit", "watches/casio", "watches/apple" };

                using (var session = store.OpenSession())
                {
                    session.Store(new Person
                    {
                        Name = "Oren",
                    }, id);

                    var tsf = session.TimeSeriesFor(id, "HeartRate");

                    var rand = new Random();

                    for (int i = 0; i < TimeSpan.FromDays(1).TotalMinutes; i++)
                    {
                        var value = rand.NextDouble();
                        tsf.Append(baseline.AddMinutes(i), value, tag: tags[i % 3]);
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var values = session.TimeSeriesFor(id, "HeartRate")
                        .Get()
                        .Where(e => e.Tag == "watches/fitbit")
                        .Select(e => e.Value)
                        .ToList();

                    values.Sort();

                    var number = new Random().NextDouble() * 100;
                    var tag = "watches/fitbit";

                    var query = session.Advanced.RawQuery<TimeSeriesAggregationResult>(@"
from People 
select timeseries(
    from HeartRate 
    between $start and $end
    where tag == $t
    select percentile($n), min(), max()
)")
                        .AddParameter("start", baseline)
                        .AddParameter("end", baseline.AddDays(1))
                        .AddParameter("t", tag)
                        .AddParameter("n", number);

                    var result = query.First();
                    Assert.Equal(1, result.Results.Length);

                    Assert.Equal(values[0], result.Results[0].Min[0]);
                    Assert.Equal(values[^1], result.Results[0].Max[0]);

                    var index = (int)Math.Ceiling((number / 100) * values.Count);
                    var expectedPercentile = values[index - 1];

                    Assert.Equal(expectedPercentile, result.Results[0].Percentile[0]);
                }
            }
        }

        [Fact]
        public void CanUsePercentileInTimeSeriesQuery_Linq()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today.EnsureUtc();
                var id = "people/1";

                var values = new List<double>
                {
                    43, 54, 56, 61, 62,
                    66, 68, 69, 69, 70,
                    71, 72, 77, 78, 79,
                    85, 87, 88, 89, 93,
                    95, 96, 98, 99, 99
                };

                using (var session = store.OpenSession())
                {
                    session.Store(new Person
                    {
                        Name = "Oren",
                    }, id);

                    var tsf = session.TimeSeriesFor(id, "HeartRate");

                    var count = values.Count;
                    var rand = new Random();

                    for (int i = 0; i < count; i++)
                    {
                        var index = rand.Next(0, values.Count - 1);

                        tsf.Append(baseline.AddHours(i), values[index]);

                        values.RemoveAt(index);
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    const double number = 90;

                    var query = session.Query<Person>()
                        .Select(p => RavenQuery.TimeSeries(p, "HeartRate")
                            .Select(x => new
                            {
                                P = x.Percentile(number)
                            })
                            .ToList());

                    var result = query.First();
                    Assert.Equal(1, result.Results.Length);

                    Assert.Equal(98, result.Results[0].Percentile[0]);

                }
            }
        }

        [Fact]
        public void CanUsePercentileInTimeSeriesQuery_WithGroupBy_Linq()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today.EnsureUtc();
                var id = "people/1";

                using (var session = store.OpenSession())
                {
                    session.Store(new Person
                    {
                        Name = "Oren",
                    }, id);

                    var tsf = session.TimeSeriesFor(id, "HeartRate");
                    var rand = new Random();

                    for (int i = 0; i < TimeSpan.FromDays(1).TotalMinutes; i++)
                    {
                        if (i % 7 == 0)
                            continue;

                        var value = rand.NextDouble();

                        tsf.Append(baseline.AddMinutes(i), value);
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var number = new Random().NextDouble() * 100;

                    var allEntries = session.TimeSeriesFor(id, "HeartRate").Get();
                    var groupByHour = allEntries
                        .GroupBy(e => new
                        {
                            e.Timestamp.Day, e.Timestamp.Hour
                        })
                        .ToList();

                    var query = session.Query<Person>()
                        .Select(p => RavenQuery.TimeSeries(p, "HeartRate")
                            .GroupBy(g => g.Hours(1))
                            .Select(x => new
                            {
                                P = x.Percentile(number)
                            })
                            .ToList());

                    var result = query.First();

                    Assert.Equal(24, result.Results.Length);

                    foreach (var rangeAggregation in result.Results)
                    {
                        var day = rangeAggregation.From.Day;
                        var hour = rangeAggregation.From.Hour;

                        var group = groupByHour.SingleOrDefault(x => x.Key.Day == day && x.Key.Hour == hour);
                        Assert.NotNull(group);

                        var groupValues = group.Select(g => g.Value).ToList();
                        groupValues.Sort();

                        var index = (int)Math.Ceiling((number / 100) * groupValues.Count);
                        var expected = groupValues[index - 1];

                        Assert.Equal(expected, rangeAggregation.Percentile[0]);
                    }
                }
            }
        }

        [Fact]
        public void CanUsePercentileInTimeSeriesQuery_WithWhere_Linq()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today.EnsureUtc();
                var id = "people/1";

                var tags = new [] { "watches/fitbit", "watches/casio", "watches/apple" };

                using (var session = store.OpenSession())
                {
                    session.Store(new Person
                    {
                        Name = "Oren",
                    }, id);

                    var tsf = session.TimeSeriesFor(id, "HeartRate");

                    var rand = new Random();

                    for (int i = 0; i < TimeSpan.FromDays(1).TotalMinutes; i++)
                    {
                        var value = rand.NextDouble();
                        tsf.Append(baseline.AddMinutes(i), value, tag: tags[i % 3]);
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var values = session.TimeSeriesFor(id, "HeartRate")
                        .Get()
                        .Where(e => e.Tag == "watches/fitbit")
                        .Select(e => e.Value)
                        .ToList();

                    values.Sort();

                    var number = new Random().NextDouble() * 100;

                    var query = session.Query<Person>()
                        .Select(p => RavenQuery.TimeSeries(p, "HeartRate", baseline, baseline.AddDays(1))
                            .Where(e => e.Tag == "watches/fitbit")
                            .Select(x => new
                            {
                                Percentile = x.Percentile(number), 
                                Min = x.Min(),
                                Max = x.Max()
                            })
                            .ToList());

                    var result = query.First();
                    Assert.Equal(1, result.Results.Length);

                    Assert.Equal(values[0], result.Results[0].Min[0]);
                    Assert.Equal(values[^1], result.Results[0].Max[0]);

                    var index = (int)Math.Ceiling((number / 100) * values.Count);
                    var expectedPercentile = values[index - 1];

                    Assert.Equal(expectedPercentile, result.Results[0].Percentile[0]);
                }
            }
        }

    }
}
