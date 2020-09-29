using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Queries.TimeSeries;
using Raven.Client.Exceptions;
using Raven.Tests.Core.Utils.Entities;
using Sparrow;
using Xunit;
using Xunit.Abstractions;
using User = SlowTests.Core.Utils.Entities.User;

namespace SlowTests.Client.TimeSeries.Issues
{
    public class RavenDB_15020 : RavenTestBase
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
                    session.Store(new Person {Name = "Oren",}, id);

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
                    session.Store(new Person {Name = "Oren",}, id);

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

                const double number = 90;

                using (var session = store.OpenSession())
                {
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

                using (var session = store.OpenSession())
                {
                    var query = session.Query<Person>()
                        .Select(p => RavenQuery.TimeSeries(p, "HeartRate")
                            .Select(x => x.Percentile(number))
                            .ToList());

                    var result = query.First();
                    Assert.Equal(1, result.Results.Length);

                    Assert.Equal(98, result.Results[0].Percentile[0]);
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Query<Person>()
                        .Select(p => RavenQuery.TimeSeries(p, "HeartRate")
                            .Select(x => new TsResult
                            {
                                Count = x.Count(),
                                Percentile = x.Percentile(number)
                            })
                            .ToList());

                    var result = query.First();
                    Assert.Equal(1, result.Results.Length);

                    Assert.Equal(98, result.Results[0].Percentile[0]);
                }
            }
        }

        [Fact]
        public void CanUsePercentileInTimeSeriesQuery_WithGroupBy()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today.EnsureUtc();
                var id = "people/1";

                using (var session = store.OpenSession())
                {
                    session.Store(new Person {Name = "Oren",}, id);

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
                        .GroupBy(e => new {e.Timestamp.Day, e.Timestamp.Hour})
                        .ToList();

                    var query = session.Query<Person>()
                        .Select(p => RavenQuery.TimeSeries(p, "HeartRate")
                            .GroupBy(g => g.Hours(1))
                            .Select(x => new {P = x.Percentile(number)})
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
        public void CanUsePercentileInTimeSeriesQuery_WithWhere()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today.EnsureUtc();
                var id = "people/1";

                var tags = new[] {"watches/fitbit", "watches/casio", "watches/apple"};

                using (var session = store.OpenSession())
                {
                    session.Store(new Person {Name = "Oren",}, id);

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
                            .Select(x => new {Percentile = x.Percentile(number), Min = x.Min(), Max = x.Max()})
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

        [Fact]
        public void CanUsePercentileInTimeSeriesQuery_WithScale()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today.EnsureUtc();
                var id = "people/1";


                using (var session = store.OpenSession())
                {
                    session.Store(new Person {Name = "Oren",}, id);

                    var tsf = session.TimeSeriesFor(id, "HeartRate");

                    var rand = new Random();
                    var scale = 1000;

                    for (int i = 0; i < TimeSpan.FromDays(1).TotalMinutes; i++)
                    {
                        var value = rand.NextDouble() * scale;
                        tsf.Append(baseline.AddMinutes(i), value);
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var values = session.TimeSeriesFor(id, "HeartRate")
                        .Get()
                        .Select(e => e.Value * 0.001)
                        .ToList();

                    values.Sort();

                    var number = new Random().NextDouble() * 100;

                    var query = session.Query<Person>()
                        .Select(p => RavenQuery.TimeSeries(p, "HeartRate", baseline, baseline.AddDays(1))
                            .Select(x => new {Percentile = x.Percentile(number), Min = x.Min(), Max = x.Max()})
                            .Scale(0.001)
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

        [Fact]
        public void CanUsePercentileInTimeSeriesQuery_WithInterpolation()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today.EnsureUtc();
                var id = "people/1";

                using (var session = store.OpenSession())
                {
                    session.Store(new Person {Name = "Oren",}, id);

                    var tsf = session.TimeSeriesFor(id, "HeartRate");

                    var rand = new Random();

                    for (int i = 0; i < TimeSpan.FromHours(4).TotalMinutes; i++)
                    {
                        if (i >= 60 && i <= 180)
                            continue;

                        var value = rand.NextDouble();
                        tsf.Append(baseline.AddMinutes(i), value);
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var number = new Random().NextDouble() * 100;

                    var query = session.Query<Person>()
                        .Select(p => RavenQuery.TimeSeries(p, "HeartRate", baseline, baseline.AddDays(1))
                            .GroupBy(g => g
                                .Hours(1)
                                .WithOptions(new TimeSeriesAggregationOptions {Interpolation = InterpolationType.Linear}))
                            .Select(x => new {Percentile = x.Percentile(number)})
                            .ToList());

                    var result = query.First();
                    Assert.Equal(4, result.Results.Length);

                    // assert percentiles in known data

                    // 00:00 - 01:00
                    Assert.Equal(baseline, result.Results[0].From);
                    Assert.Equal(baseline.AddHours(1), result.Results[0].To);

                    var values = session.TimeSeriesFor(id, "HeartRate")
                        .Get(baseline, baseline.AddHours(1))
                        .Select(e => e.Value)
                        .ToList();

                    values.Sort();
                    var index = (int)Math.Ceiling((number / 100) * values.Count);
                    var expectedPercentile = values[index - 1];

                    Assert.Equal(expectedPercentile, result.Results[0].Percentile[0]);

                    // 03:00 - 04:00
                    Assert.Equal(baseline.AddHours(3), result.Results[3].From);
                    Assert.Equal(baseline.AddHours(4), result.Results[3].To);

                    values = session.TimeSeriesFor(id, "HeartRate")
                        .Get(baseline.AddHours(3), baseline.AddHours(4))
                        .Select(e => e.Value)
                        .ToList();

                    values.Sort();

                    index = (int)Math.Ceiling((number / 100) * values.Count);
                    expectedPercentile = values[index - 1];

                    Assert.Equal(expectedPercentile, result.Results[3].Percentile[0]);

                    // assert percentiles in filled up gaps

                    // 01:00 - 02:00
                    Assert.Equal(baseline.AddHours(1), result.Results[1].From);
                    Assert.Equal(baseline.AddHours(2), result.Results[1].To);

                    var dy = (result.Results[3].Percentile[0] - result.Results[0].Percentile[0]);
                    double quotient = 1d / 3;
                    expectedPercentile = result.Results[0].Percentile[0] + dy * quotient;

                    Assert.Equal(expectedPercentile, result.Results[1].Percentile[0]);

                    // 02:00 - 03:00
                    Assert.Equal(baseline.AddHours(2), result.Results[2].From);
                    Assert.Equal(baseline.AddHours(3), result.Results[2].To);

                    quotient = 2d / 3;
                    expectedPercentile = result.Results[0].Percentile[0] + dy * quotient;

                    Assert.Equal(expectedPercentile, result.Results[2].Percentile[0]);
                }
            }
        }

        [Fact]
        public async Task ShouldThrowOnUsingPercentileOnRolledUpSeries()
        {
            using (var store = GetDocumentStore())
            {
                var raw = new RawTimeSeriesPolicy(TimeSpan.FromHours(24));

                var p1 = new TimeSeriesPolicy("By6Hours", TimeSpan.FromHours(6));

                var config = new TimeSeriesConfiguration
                {
                    Collections = new Dictionary<string, TimeSeriesCollectionConfiguration>
                    {
                        ["Users"] = new TimeSeriesCollectionConfiguration
                        {
                            RawPolicy = raw,
                            Policies = new List<TimeSeriesPolicy>
                            {
                                p1
                            }
                        },
                    },
                    PolicyCheckFrequency = TimeSpan.FromSeconds(1)
                };
                await store.Maintenance.SendAsync(new ConfigureTimeSeriesOperation(config));
                var database = await GetDocumentDatabaseInstanceFor(store);

                var now = DateTime.Today.EnsureUtc();

                var baseline = now.AddDays(-12);
                var total = TimeSpan.FromDays(12).TotalMinutes;

                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Karmel"
                    }, "users/karmel");

                    var ts = session.TimeSeriesFor("users/karmel", "Heartrate");
                    for (int i = 0; i <= total; i++)
                    {
                        ts.Append(baseline.AddMinutes(i), i);
                    }

                    session.SaveChanges();
                }

                await database.TimeSeriesPolicyRunner.RunRollups();
                await database.TimeSeriesPolicyRunner.DoRetention();

                using (var session = store.OpenSession())
                {
                    var query = session.Query<User>()
                        .Select(u => RavenQuery.TimeSeries(u, "Heartrate")
                            .GroupBy(g => g.Days(1))
                            .Select(agg => new
                            {
                                Max = agg.Max(), 
                                Min = agg.Min(), 
                                P25 = agg.Percentile(25)
                            })
                            .ToList());

                    var ex = Assert.Throws<RavenException>(() => query.Single());
                    Assert.NotNull(ex.InnerException);
                    Assert.Contains("Cannot use aggregation method 'Percentile' on rolled-up time series", ex.InnerException.Message);
                }
            }
        }

        [Fact]
        public void CanUseSlopeInTimeSeriesQuery_Linq()
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

                    for (int i = 0; i < TimeSpan.FromDays(1).TotalMinutes; i++)
                    {
                        tsf.Append(baseline.AddMinutes(i), i * 100);
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Query<Person>()
                        .Select(p => RavenQuery.TimeSeries(p, "HeartRate")
                            .GroupBy(g => g.Hours(1))
                            .Select(x => new
                            {
                                Slope = x.Slope()
                            })
                            .ToList());

                    var result = query.First();
                    Assert.Equal(24, result.Results.Length);

                    var dx = TimeSpan.FromMinutes(60).TotalMilliseconds;
                    var dy = 5900;
                    var expected = dy / dx;
                    foreach (var rangeAggregation in result.Results)
                    {
                        Assert.Equal(expected, rangeAggregation.Slope[0]);
                    }
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Query<Person>()
                        .Select(p => RavenQuery.TimeSeries(p, "HeartRate")
                            .GroupBy(g => g.Hours(1))
                            .Select(x => x.Slope())
                            .ToList());

                    var result = query.First();
                    Assert.Equal(24, result.Results.Length);

                    var dx = TimeSpan.FromMinutes(60).TotalMilliseconds;
                    var dy = 5900;
                    var expected = dy / dx;
                    foreach (var rangeAggregation in result.Results)
                    {
                        Assert.Equal(expected, rangeAggregation.Slope[0]);
                    }
                }
            }
        }

        [Fact]
        public void CanUseSlopeInTimeSeriesQuery_Raw()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today.EnsureUtc();
                var id = "people/1";

                using (var session = store.OpenSession())
                {
                    session.Store(new Person {Name = "Oren",}, id);

                    var tsf = session.TimeSeriesFor(id, "HeartRate");

                    for (int i = 0; i < TimeSpan.FromDays(1).TotalMinutes; i++)
                    {
                        tsf.Append(baseline.AddMinutes(i), i * 100);
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesAggregationResult>(
@"
from People as p 
select timeseries(
    from p.HeartRate
    group by 1 hour
    select slope()
)");

                    var result = query.First();
                    Assert.Equal(24, result.Results.Length);

                    var dx = TimeSpan.FromMinutes(60).TotalMilliseconds;
                    var dy = 5900;
                    var expected = dy / dx;
                    foreach (var rangeAggregation in result.Results)
                    {
                        Assert.Equal(expected, rangeAggregation.Slope[0]);
                    }
                }
            }
        }

        [Fact]
        public void ShouldThrowOnUsingSlopeWithoutGroupBy()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today.EnsureUtc();
                var id = "people/1";

                using (var session = store.OpenSession())
                {
                    session.Store(new Person {Name = "Oren",}, id);

                    var tsf = session.TimeSeriesFor(id, "HeartRate");

                    for (int i = 0; i < TimeSpan.FromDays(1).TotalMinutes; i++)
                    {
                        tsf.Append(baseline.AddMinutes(i), i * 100, "tag");
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Query<Person>()
                        .Select(p => RavenQuery.TimeSeries(p, "HeartRate")
                            .Where(e => e.Tag != null)
                            .Select(x => new
                            {
                                Slope = x.Slope()
                            })
                            .ToList());

                    var ex = Assert.Throws<RavenException>(() => query.First());

                    Assert.Contains("Cannot use aggregation method 'Slope' without having a 'GroupBy' clause", ex.Message);
                }
            }
        }

        [Fact]
        public void CanUseSlopeInTimeSeriesQuery_WithInterpolation()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today.EnsureUtc();
                var id = "people/1";

                using (var session = store.OpenSession())
                {
                    session.Store(new Person {Name = "Oren",}, id);

                    var tsf = session.TimeSeriesFor(id, "HeartRate");

                    for (int i = 0; i < TimeSpan.FromHours(3).TotalMinutes; i++)
                    {
                        if (i >= 60 && i < 120)
                            continue;

                        tsf.Append(baseline.AddMinutes(i), i * 100);
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Query<Person>()
                        .Select(p => RavenQuery.TimeSeries(p, "HeartRate")
                            .GroupBy(g => g
                                .Hours(1)
                                .WithOptions(new TimeSeriesAggregationOptions
                                {
                                    Interpolation = InterpolationType.Linear
                                }))
                            .Select(x => new
                            {
                                Slope = x.Slope()
                            })
                            .ToList());

                    var result = query.First();
                    Assert.Equal(3, result.Results.Length);

                    var dx = TimeSpan.FromMinutes(60).TotalMilliseconds;
                    var dy = 5900;
                    var expected = dy / dx;
                    foreach (var rangeAggregation in result.Results)
                    {
                        Assert.Equal(expected, rangeAggregation.Slope[0]);
                    }
                }
            }
        }

        [Fact]
        public void CanUseSlopeInTimeSeriesQuery_WithScale()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today.EnsureUtc();
                var id = "people/1";


                using (var session = store.OpenSession())
                {
                    session.Store(new Person {Name = "Oren",}, id);

                    var tsf = session.TimeSeriesFor(id, "HeartRate");

                    for (int i = 0; i < TimeSpan.FromDays(1).TotalMinutes; i++)
                    {
                        tsf.Append(baseline.AddMinutes(i), i * 100);
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var scale = 1000;
                    var query = session.Query<Person>()
                        .Select(p => RavenQuery.TimeSeries(p, "HeartRate")
                            .GroupBy(g => g.Hours(1))
                            .Select(x => new
                            {
                                Slope = x.Slope()
                            })
                            .Scale(scale)
                            .ToList());

                    var result = query.First();
                    Assert.Equal(24, result.Results.Length);
                    
                    var dy = 5900;
                    var dx = TimeSpan.FromMinutes(60).TotalMilliseconds; 
                    var dxSeconds = TimeSpan.FromMinutes(60).TotalSeconds;
                    var expected = dy / dxSeconds;
                    Assert.Equal(expected, scale * (dy / dx));

                    foreach (var rangeAggregation in result.Results)
                    {
                        Assert.Equal(expected, rangeAggregation.Slope[0]);
                    }
                }
            }
        }

        [Fact]
        public async Task CanUseSlopeInTimeSeriesQuery_OnRollUps()
        {
            using (var store = GetDocumentStore())
            {
                var raw = new RawTimeSeriesPolicy(TimeSpan.FromHours(24));

                var p1 = new TimeSeriesPolicy("By6Hours", TimeSpan.FromHours(6));

                var config = new TimeSeriesConfiguration
                {
                    Collections = new Dictionary<string, TimeSeriesCollectionConfiguration>
                    {
                        ["Users"] = new TimeSeriesCollectionConfiguration
                        {
                            RawPolicy = raw,
                            Policies = new List<TimeSeriesPolicy>
                            {
                                p1
                            }
                        },
                    },
                    PolicyCheckFrequency = TimeSpan.FromSeconds(1)
                };
                await store.Maintenance.SendAsync(new ConfigureTimeSeriesOperation(config));
                var database = await GetDocumentDatabaseInstanceFor(store);

                var now = DateTime.Today.EnsureUtc();

                var baseline = now.AddDays(-12);
                var total = TimeSpan.FromDays(12).TotalMinutes;

                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Karmel"
                    }, "users/karmel");

                    var ts = session.TimeSeriesFor("users/karmel", "Heartrate");
                    for (int i = 0; i <= total; i++)
                    {
                        ts.Append(baseline.AddMinutes(i), i);
                    }

                    session.SaveChanges();
                }

                await database.TimeSeriesPolicyRunner.RunRollups();
                await database.TimeSeriesPolicyRunner.DoRetention();

                using (var session = store.OpenSession())
                {
                    var query = session.Query<User>()
                        .Select(u => RavenQuery.TimeSeries(u, "Heartrate")
                            .GroupBy(g => g.Hours(1))
                            .Select(agg => new
                            {
                                First = agg.First(),
                                Last = agg.Last(),
                                Slope = agg.Slope()
                            })
                            .ToList());

                    var result = query.Single();

                    Assert.Equal(total + 1, result.Count);

                    foreach (var rangeAggregation in result.Results)
                    {
                        var dy = rangeAggregation.Last[0] - rangeAggregation.First[0];
                        var dx = (rangeAggregation.To - rangeAggregation.From).TotalMilliseconds;
                        var expected = dy / dx;

                        Assert.Equal(expected, rangeAggregation.Slope[0]);
                    }
                }
            }
        }

        [Fact]
        public void CanUseStdDevInTimeSeriesQuery_RawQuery()
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
                    session.Store(new Person { Name = "Oren", }, id);

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
                    var query = session.Advanced.RawQuery<TimeSeriesAggregationResult>(@"
from People 
select timeseries(
    from HeartRate 
    select stddev()
)");

                    var result = query.First();
                    Assert.Equal(1, result.Results.Length);

                    var allValues = session.TimeSeriesFor(id, "HeartRate")
                        .Get()
                        .Select(entry => entry.Value)
                        .ToList();

                    var mean = allValues.Average();
                    var sum = allValues.Sum(v => (v - mean) * (v - mean));
                    var expected = Math.Sqrt(sum / (allValues.Count - 1));

                    Assert.Equal(expected, result.Results[0].StandardDeviation[0]);
                }
            }
        }

        [Fact]
        public void CanUseStdDevInTimeSeriesQuery_Linq()
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

                var allValues = store.Operations
                    .Send(new GetTimeSeriesOperation(id, "HeartRate"))
                    .Entries
                    .Select(entry => entry.Value)
                    .ToList();

                var mean = allValues.Average();
                var sigma = allValues.Sum(v => Math.Pow(v - mean, 2));
                var expected = Math.Sqrt(sigma / (allValues.Count - 1));

                using (var session = store.OpenSession())
                {
                    var query = session.Query<Person>()
                        .Select(p => RavenQuery.TimeSeries(p, "HeartRate")
                            .Select(x => new
                            {
                                StdDev = x.StandardDeviation()
                            })
                            .ToList());

                    var result = query.First();

                    Assert.Equal(1, result.Results.Length);
                    Assert.Equal(expected, result.Results[0].StandardDeviation[0]);
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Query<Person>()
                        .Select(p => RavenQuery.TimeSeries(p, "HeartRate")
                            .Select(x => x.StandardDeviation())
                            .ToList());

                    var result = query.First();
                    Assert.Equal(1, result.Results.Length);
                    Assert.Equal(expected, result.Results[0].StandardDeviation[0]);
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Query<Person>()
                        .Select(p => RavenQuery.TimeSeries(p, "HeartRate")
                            .Select(x => new TsResult
                            {
                                Count = x.Count(),
                                StdDev = x.StandardDeviation()
                            })
                            .ToList());

                    var result = query.First();
                    Assert.Equal(1, result.Results.Length);
                    Assert.Equal(expected, result.Results[0].StandardDeviation[0]);
                }
            }
        }

        [Fact]
        public void CanUseStdDevInTimeSeriesQuery_WithGroupBy()
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
                    var allEntries = session.TimeSeriesFor(id, "HeartRate").Get();
                    var groupByHour = allEntries
                        .GroupBy(e => new { e.Timestamp.Day, e.Timestamp.Hour })
                        .ToList();

                    var query = session.Query<Person>()
                        .Select(p => RavenQuery.TimeSeries(p, "HeartRate")
                            .GroupBy(g => g.Hours(1))
                            .Select(x => x.StandardDeviation())
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
                        var mean = groupValues.Average();
                        var sigma = groupValues.Sum(v => Math.Pow(v - mean, 2));
                        var expected = Math.Sqrt(sigma / (groupValues.Count - 1));
                         
                        Assert.Equal(expected, rangeAggregation.StandardDeviation[0]);
                    }
                }
            }
        }

        [Fact]
        public void CanUseStdDevInTimeSeriesQuery_WithWhere()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today.EnsureUtc();
                var id = "people/1";

                var tags = new[] { "watches/fitbit", "watches/casio", "watches/apple" };

                using (var session = store.OpenSession())
                {
                    session.Store(new Person { Name = "Oren", }, id);

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
                    
                    var mean = values.Average();
                    var sigma = values.Sum(v => Math.Pow(v - mean, 2));
                    var expected = Math.Sqrt(sigma / (values.Count - 1));

                    var query = session.Query<Person>()
                        .Select(p => RavenQuery.TimeSeries(p, "HeartRate", baseline, baseline.AddDays(1))
                            .Where(e => e.Tag == "watches/fitbit")
                            .Select(x => new
                            {
                                StandardDev = x.StandardDeviation(), 
                                Min = x.Min(), 
                                Max = x.Max()
                            })
                            .ToList());

                    var result = query.First();
                    Assert.Equal(1, result.Results.Length);

                    values.Sort();
                    Assert.Equal(values[0], result.Results[0].Min[0]);
                    Assert.Equal(values[^1], result.Results[0].Max[0]);

                    Assert.Equal(expected, result.Results[0].StandardDeviation[0]);
                }
            }
        }

        [Fact]
        public void CanUseStdDevInTimeSeriesQuery_WithScale()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today.EnsureUtc();
                var id = "people/1";


                using (var session = store.OpenSession())
                {
                    session.Store(new Person { Name = "Oren", }, id);

                    var tsf = session.TimeSeriesFor(id, "HeartRate");

                    var rand = new Random();
                    var scale = 1000;

                    for (int i = 0; i < TimeSpan.FromDays(1).TotalMinutes; i++)
                    {
                        var value = rand.NextDouble() * scale;
                        tsf.Append(baseline.AddMinutes(i), value);
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var scaledValues = session.TimeSeriesFor(id, "HeartRate")
                        .Get()
                        .Select(e => e.Value * 0.001)
                        .ToList();

                    var mean = scaledValues.Average();
                    var sigma = scaledValues.Sum(v => Math.Pow(v - mean, 2));
                    var expected = Math.Sqrt(sigma / (scaledValues.Count - 1));

                    var query = session.Query<Person>()
                        .Select(p => RavenQuery.TimeSeries(p, "HeartRate", baseline, baseline.AddDays(1))
                            .Select(x => new
                            {
                                StandardDeviation = x.StandardDeviation(), 
                                Min = x.Min(), 
                                Max = x.Max()
                            })
                            .Scale(0.001)
                            .ToList());

                    var result = query.First();
                    Assert.Equal(1, result.Results.Length);

                    scaledValues.Sort();
                    Assert.Equal(scaledValues[0], result.Results[0].Min[0]);
                    Assert.Equal(scaledValues[^1], result.Results[0].Max[0]);

                    var tolerance = 0.0000000001;
                    var diff = Math.Abs(expected - result.Results[0].StandardDeviation[0]);
                    Assert.True(diff < tolerance);
                }
            }
        }

        [Fact]
        public void CanUseStdDevInTimeSeriesQuery_WithInterpolation()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today.EnsureUtc();
                var id = "people/1";

                using (var session = store.OpenSession())
                {
                    session.Store(new Person { Name = "Oren", }, id);

                    var tsf = session.TimeSeriesFor(id, "HeartRate");

                    var rand = new Random();

                    for (int i = 0; i < TimeSpan.FromHours(4).TotalMinutes; i++)
                    {
                        if (i >= 60 && i <= 180)
                            continue;

                        var value = rand.NextDouble();
                        tsf.Append(baseline.AddMinutes(i), value);
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Query<Person>()
                        .Select(p => RavenQuery.TimeSeries(p, "HeartRate", baseline, baseline.AddDays(1))
                            .GroupBy(g => g
                                .Hours(1)
                                .WithOptions(new TimeSeriesAggregationOptions { Interpolation = InterpolationType.Linear }))
                            .Select(x => x.StandardDeviation())
                            .ToList());

                    var result = query.First();
                    Assert.Equal(4, result.Results.Length);

                    // assert standard dev in known data

                    // 00:00 - 01:00
                    Assert.Equal(baseline, result.Results[0].From);
                    Assert.Equal(baseline.AddHours(1), result.Results[0].To);

                    var values = session.TimeSeriesFor(id, "HeartRate")
                        .Get(baseline, baseline.AddHours(1))
                        .Select(e => e.Value)
                        .ToList();

                    var mean = values.Average();
                    var sigma = values.Sum(v => Math.Pow(v - mean, 2));
                    var expected = Math.Sqrt(sigma / (values.Count - 1));

                    Assert.Equal(expected, result.Results[0].StandardDeviation[0]);

                    // 03:00 - 04:00
                    Assert.Equal(baseline.AddHours(3), result.Results[3].From);
                    Assert.Equal(baseline.AddHours(4), result.Results[3].To);

                    values = session.TimeSeriesFor(id, "HeartRate")
                        .Get(baseline.AddHours(3), baseline.AddHours(4))
                        .Select(e => e.Value)
                        .ToList();

                    mean = values.Average();
                    sigma = values.Sum(v => Math.Pow(v - mean, 2));
                    expected = Math.Sqrt(sigma / (values.Count - 1));

                    Assert.Equal(expected, result.Results[3].StandardDeviation[0]);

                    // assert percentiles in filled up gaps

                    // 01:00 - 02:00
                    Assert.Equal(baseline.AddHours(1), result.Results[1].From);
                    Assert.Equal(baseline.AddHours(2), result.Results[1].To);

                    var dy = (result.Results[3].StandardDeviation[0] - result.Results[0].StandardDeviation[0]);
                    double quotient = 1d / 3;
                    expected = result.Results[0].StandardDeviation[0] + dy * quotient;

                    Assert.Equal(expected, result.Results[1].StandardDeviation[0]);

                    // 02:00 - 03:00
                    Assert.Equal(baseline.AddHours(2), result.Results[2].From);
                    Assert.Equal(baseline.AddHours(3), result.Results[2].To);

                    quotient = 2d / 3;
                    expected = result.Results[0].StandardDeviation[0] + dy * quotient;

                    Assert.Equal(expected, result.Results[2].StandardDeviation[0]);
                }
            }
        }

        [Fact]
        public async Task ShouldThrowOnUsingStdDevOnRolledUpSeries()
        {
            using (var store = GetDocumentStore())
            {
                var raw = new RawTimeSeriesPolicy(TimeSpan.FromHours(24));

                var p1 = new TimeSeriesPolicy("By6Hours", TimeSpan.FromHours(6));

                var config = new TimeSeriesConfiguration
                {
                    Collections = new Dictionary<string, TimeSeriesCollectionConfiguration>
                    {
                        ["Users"] = new TimeSeriesCollectionConfiguration
                        {
                            RawPolicy = raw,
                            Policies = new List<TimeSeriesPolicy>
                            {
                                p1
                            }
                        },
                    },
                    PolicyCheckFrequency = TimeSpan.FromSeconds(1)
                };
                await store.Maintenance.SendAsync(new ConfigureTimeSeriesOperation(config));
                var database = await GetDocumentDatabaseInstanceFor(store);

                var now = DateTime.Today.EnsureUtc();

                var baseline = now.AddDays(-12);
                var total = TimeSpan.FromDays(12).TotalMinutes;

                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Karmel"
                    }, "users/karmel");

                    var ts = session.TimeSeriesFor("users/karmel", "Heartrate");
                    for (int i = 0; i <= total; i++)
                    {
                        ts.Append(baseline.AddMinutes(i), i);
                    }

                    session.SaveChanges();
                }

                await database.TimeSeriesPolicyRunner.RunRollups();
                await database.TimeSeriesPolicyRunner.DoRetention();

                using (var session = store.OpenSession())
                {
                    var query = session.Query<User>()
                        .Select(u => RavenQuery.TimeSeries(u, "Heartrate")
                            .GroupBy(g => g.Days(1))
                            .Select(agg => new
                            {
                                Max = agg.Max(),
                                Min = agg.Min(),
                                StandardDev = agg.StandardDeviation()
                            })
                            .ToList());

                    var ex = Assert.Throws<RavenException>(() => query.Single());
                    Assert.NotNull(ex.InnerException);
                    Assert.Contains("Cannot use aggregation method 'StandardDeviation' on rolled-up time series", ex.InnerException.Message);
                }
            }
        }

        private class TsResult
        {
            public double[] Percentile { get; set; }

            public long[] Count { get; set; }

            public double[] StdDev { get; set; }

        }
    }
}
