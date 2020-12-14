using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Queries.TimeSeries;
using Raven.Tests.Core.Utils.Entities;
using Sparrow;
using Sparrow.Extensions;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client.TimeSeries.Query
{
    public class TimeSeriesDocumentQuery : RavenTestBase
    {
        public TimeSeriesDocumentQuery(ITestOutputHelper output) : base(output)
        {
        }

        private class Person
        {
            public string Id { get; set; }

            public string Name { get; set; }

            public int Age { get; set; }
        }

        private class Watch
        {
            public double Accuracy { get; set; }
        }

        [Fact]
        public void CanQueryTimeSeriesUsingDocumentQuery()
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

                    var tsf = session.TimeSeriesFor("users/ayende", "Heartrate");
                    tsf.Append(baseline.AddMinutes(61), 59, "watches/fitbit");
                    tsf.Append(baseline.AddMinutes(62), 79, "watches/apple");
                    tsf.Append(baseline.AddMinutes(63), 69, "watches/fitbit");

                    tsf.Append(baseline.AddMonths(1).AddMinutes(61), 159, "watches/apple");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(62), 179, "watches/apple");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(63), 169, "watches/fitbit");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    string tsQueryText = @"
from Heartrate between $start and $end
where Tag = 'watches/fitbit'
group by '1 month'
select min(), max(), avg()
";

                    var query = session.Advanced.DocumentQuery<User>()
                        .WhereGreaterThan(u => u.Age, 21)
                        .SelectTimeSeries(builder => builder.Raw<TimeSeriesAggregationResult>(tsQueryText))
                        .AddParameter("start", baseline.EnsureUtc())
                        .AddParameter("end", baseline.AddMonths(3).EnsureUtc());

                    var result = query.ToList();

                    Assert.Equal(1, result.Count);
                    Assert.Equal(3, result[0].Count);

                    var agg = result[0].Results;

                    Assert.Equal(2, agg.Length);

                    Assert.Equal(69, agg[0].Max[0]);
                    Assert.Equal(59, agg[0].Min[0]);
                    Assert.Equal(64, agg[0].Average[0]);

                    Assert.Equal(169, agg[1].Max[0]);
                    Assert.Equal(169, agg[1].Min[0]);
                    Assert.Equal(169, agg[1].Average[0]);
                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesRawValuesUsingDocumentQuery()
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

                    var tsf = session.TimeSeriesFor("users/ayende", "Heartrate");
                    tsf.Append(baseline.AddMinutes(61), 59, "watches/fitbit");
                    tsf.Append(baseline.AddMinutes(62), 79, "watches/apple");
                    tsf.Append(baseline.AddMinutes(63), 69, "watches/fitbit");

                    tsf.Append(baseline.AddMonths(1).AddMinutes(61), 159, "watches/apple");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(62), 179, "watches/apple");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(63), 169, "watches/fitbit");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    string tsQueryText = @"
from Heartrate between $start and $end
where Tag = 'watches/fitbit'
";

                    var query = session.Advanced.DocumentQuery<User>()
                        .WhereGreaterThan(u => u.Age, 21)
                        .SelectTimeSeries(builder => builder.Raw<TimeSeriesRawResult>(tsQueryText))
                        .AddParameter("start", baseline.EnsureUtc())
                        .AddParameter("end", baseline.AddMonths(3).EnsureUtc());

                    var result = query.ToList();

                    Assert.Equal(1, result.Count);
                    Assert.Equal(3, result[0].Count);

                    var values = result[0].Results;

                    Assert.Equal(3, values.Length);

                    Assert.Equal(new[] { 59d }, values[0].Values);
                    Assert.Equal("watches/fitbit", values[0].Tag);
                    Assert.Equal(baseline.AddMinutes(61), values[0].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    Assert.Equal(new[] { 69d }, values[1].Values);
                    Assert.Equal("watches/fitbit", values[1].Tag);
                    Assert.Equal(baseline.AddMinutes(63), values[1].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    Assert.Equal(new[] { 169d }, values[2].Values);
                    Assert.Equal("watches/fitbit", values[2].Tag);
                    Assert.Equal(baseline.AddMonths(1).AddMinutes(63), values[2].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                }
            }
        }

        [Fact]
        public async Task CanQueryTimeSeriesUsingDocumentQuery_Async()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User
                    {
                        Name = "Oren",
                        Age = 35
                    }, "users/ayende");

                    var tsf = session.TimeSeriesFor("users/ayende", "Heartrate");
                    tsf.Append(baseline.AddMinutes(61), 59, "watches/fitbit");
                    tsf.Append(baseline.AddMinutes(62), 79, "watches/apple");
                    tsf.Append(baseline.AddMinutes(63), 69, "watches/fitbit");

                    tsf.Append(baseline.AddMonths(1).AddMinutes(61), 159, "watches/apple");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(62), 179, "watches/apple");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(63), 169, "watches/fitbit");

                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    string tsQueryText = @"
from Heartrate between $start and $end
where Tag = 'watches/fitbit'
group by '1 month'
select min(), max(), avg()
";

                    var query = session.Advanced.AsyncDocumentQuery<User>()
                        .WhereGreaterThan(u => u.Age, 21)
                        .SelectTimeSeries(builder => builder.Raw<TimeSeriesAggregationResult>(tsQueryText))
                        .AddParameter("start", baseline.EnsureUtc())
                        .AddParameter("end", baseline.AddMonths(3).EnsureUtc());

                    var result = await query.ToListAsync();

                    Assert.Equal(1, result.Count);
                    Assert.Equal(3, result[0].Count);

                    var agg = result[0].Results;

                    Assert.Equal(2, agg.Length);

                    Assert.Equal(69, agg[0].Max[0]);
                    Assert.Equal(59, agg[0].Min[0]);
                    Assert.Equal(64, agg[0].Average[0]);

                    Assert.Equal(169, agg[1].Max[0]);
                    Assert.Equal(169, agg[1].Min[0]);
                    Assert.Equal(169, agg[1].Average[0]);
                }
            }
        }

        [Fact]
        public async Task CanQueryTimeSeriesRawValuesUsingDocumentQuery_Async()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User
                    {
                        Name = "Oren",
                        Age = 35
                    }, "users/ayende");

                    var tsf = session.TimeSeriesFor("users/ayende", "Heartrate");
                    tsf.Append(baseline.AddMinutes(61), 59, "watches/fitbit");
                    tsf.Append(baseline.AddMinutes(62), 79, "watches/apple");
                    tsf.Append(baseline.AddMinutes(63), 69, "watches/fitbit");

                    tsf.Append(baseline.AddMonths(1).AddMinutes(61), 159, "watches/apple");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(62), 179, "watches/apple");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(63), 169, "watches/fitbit");

                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    string tsQueryText = @"
from Heartrate between $start and $end
where Tag = 'watches/fitbit'
";

                    var query = session.Advanced.AsyncDocumentQuery<User>()
                        .WhereGreaterThan(u => u.Age, 21)
                        .SelectTimeSeries(builder => builder.Raw<TimeSeriesRawResult>(tsQueryText))
                        .AddParameter("start", baseline.EnsureUtc())
                        .AddParameter("end", baseline.AddMonths(3).EnsureUtc());

                    var result = await query.ToListAsync();

                    Assert.Equal(1, result.Count);
                    Assert.Equal(3, result[0].Count);

                    var values = result[0].Results;

                    Assert.Equal(3, values.Length);

                    Assert.Equal(new[] { 59d }, values[0].Values);
                    Assert.Equal("watches/fitbit", values[0].Tag);
                    Assert.Equal(baseline.AddMinutes(61), values[0].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    Assert.Equal(new[] { 69d }, values[1].Values);
                    Assert.Equal("watches/fitbit", values[1].Tag);
                    Assert.Equal(baseline.AddMinutes(63), values[1].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    Assert.Equal(new[] { 169d }, values[2].Values);
                    Assert.Equal("watches/fitbit", values[2].Tag);
                    Assert.Equal(baseline.AddMonths(1).AddMinutes(63), values[2].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                }
            }
        }

        [Fact]
        public void TimeSeriesDocumentQuery_UsingBuilder()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today.EnsureUtc();

                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Oren",
                        Age = 35
                    }, "users/ayende");

                    var tsf = session.TimeSeriesFor("users/ayende", "Heartrate");

                    tsf.Append(baseline, 59, "watches/fitbit");
                    tsf.Append(baseline.AddHours(1), 69, "watches/fitbit");
                    tsf.Append(baseline.AddHours(2), 79, "watches/fitbit");

                    tsf.Append(baseline.AddHours(3), 89, "watches/fitbit");
                    tsf.Append(baseline.AddHours(4), 99, "watches/fitbit");
                    tsf.Append(baseline.AddHours(5), 109, "watches/fitbit");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.DocumentQuery<User>()
                        .WhereGreaterThan(u => u.Age, 21)
                        .SelectTimeSeries(builder => builder.From("Heartrate").ToList());

                    var result = query.First();

                    Assert.Equal(6, result.Count);

                    for (var i = 0; i < result.Results.Length; i++)
                    {
                        var entry = result.Results[i];
                        Assert.Equal(baseline.AddHours(i), entry.Timestamp);
                        Assert.Equal(59 + i * 10, entry.Value);
                        Assert.Equal("watches/fitbit", entry.Tag);
                    }
                }
            }
        }

        [Fact]
        public void TimeSeriesDocumentQuery_UsingBuilder_Between()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today.EnsureUtc();

                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Oren",
                        Age = 35
                    }, "users/ayende");

                    var tsf = session.TimeSeriesFor("users/ayende", "Heartrate");

                    tsf.Append(baseline, 59, "watches/fitbit");
                    tsf.Append(baseline.AddHours(1), 69, "watches/fitbit");
                    tsf.Append(baseline.AddHours(2), 79, "watches/fitbit");

                    tsf.Append(baseline.AddHours(3), 89, "watches/fitbit");
                    tsf.Append(baseline.AddHours(4), 99, "watches/fitbit");
                    tsf.Append(baseline.AddHours(5), 109, "watches/fitbit");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.DocumentQuery<User>()
                        .WhereGreaterThan(u => u.Age, 21)
                        .SelectTimeSeries(builder => builder
                            .From("Heartrate")
                            .Between(baseline.AddHours(4), baseline.AddDays(1))
                            .ToList());

                    var result = query.First();

                    Assert.Equal(2, result.Count);

                    var entry = result.Results[0];
                    Assert.Equal(baseline.AddHours(4), entry.Timestamp);
                    Assert.Equal(99, entry.Value);

                    entry = result.Results[1];
                    Assert.Equal(baseline.AddHours(5), entry.Timestamp);
                    Assert.Equal(109, entry.Value);
                }
            }
        }

        [Fact]
        public void TimeSeriesDocumentQuery_UsingBuilder_Where()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today.EnsureUtc();

                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Oren",
                        Age = 35
                    }, "users/ayende");

                    var tsf = session.TimeSeriesFor("users/ayende", "Heartrate");
                    tsf.Append(baseline.AddMinutes(61), 59, "watches/fitbit");
                    tsf.Append(baseline.AddMinutes(62), 79, "watches/apple");
                    tsf.Append(baseline.AddMinutes(63), 69, "watches/fitbit");

                    tsf.Append(baseline.AddMonths(1).AddMinutes(61), 159, "watches/apple");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(62), 179, "watches/apple");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(63), 169, "watches/fitbit");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.DocumentQuery<User>()
                        .WhereGreaterThan(u => u.Age, 21)
                        .SelectTimeSeries(builder => builder
                            .From("Heartrate")
                            .Between(baseline, baseline.AddMonths(3))
                            .Where(x => x.Tag == "watches/fitbit")
                            .ToList());

                    var result = query.First();

                    Assert.Equal(3, result.Count);

                    var entry = result.Results[0];
                    Assert.Equal(baseline.AddMinutes(61), entry.Timestamp);
                    Assert.Equal(59, entry.Value);

                    entry = result.Results[1];
                    Assert.Equal(baseline.AddMinutes(63), entry.Timestamp);
                    Assert.Equal(69, entry.Value);

                    entry = result.Results[2];
                    Assert.Equal(baseline.AddMonths(1).AddMinutes(63), entry.Timestamp);
                    Assert.Equal(169, entry.Value);

                    Assert.True(result.Results.All(e => e.Tag == "watches/fitbit"));
                }
            }
        }

        [Fact]
        public void TimeSeriesDocumentQuery_UsingBuilder_GroupBy()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today.EnsureUtc();

                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Oren",
                        Age = 35
                    }, "users/ayende");

                    var tsf = session.TimeSeriesFor("users/ayende", "Heartrate");
                    tsf.Append(baseline.AddMinutes(61), 59, "watches/fitbit");
                    tsf.Append(baseline.AddMinutes(62), 79, "watches/apple");
                    tsf.Append(baseline.AddMinutes(63), 69, "watches/fitbit");

                    tsf.Append(baseline.AddMonths(1).AddMinutes(61), 159, "watches/apple");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(62), 179, "watches/apple");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(63), 169, "watches/fitbit");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.DocumentQuery<User>()
                        .WhereGreaterThan(u => u.Age, 21)
                        .SelectTimeSeries(builder => builder
                            .From("Heartrate")
                            .Between(baseline, baseline.AddMonths(3))
                            .Where(x => x.Tag == "watches/fitbit")
                            .GroupBy("1h")
                            .ToList());

                    var result = query.ToList();

                    Assert.Equal(1, result.Count);
                    Assert.Equal(3, result[0].Count);

                    var agg = result[0].Results;

                    Assert.Equal(2, agg.Length);

                    Assert.Equal(69, agg[0].Max[0]);
                    Assert.Equal(59, agg[0].Min[0]);
                    Assert.Equal(64, agg[0].Average[0]);

                    Assert.Equal(169, agg[1].Max[0]);
                    Assert.Equal(169, agg[1].Min[0]);
                    Assert.Equal(169, agg[1].Average[0]);
                }
            }
        }

        [Fact]
        public void TimeSeriesDocumentQuery_UsingBuilder_StronglyTypedGroupBy()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today.EnsureUtc();

                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Oren",
                        Age = 35
                    }, "users/ayende");

                    var tsf = session.TimeSeriesFor("users/ayende", "Heartrate");
                    tsf.Append(baseline.AddMinutes(61), 59, "watches/fitbit");
                    tsf.Append(baseline.AddMinutes(62), 79, "watches/apple");
                    tsf.Append(baseline.AddMinutes(63), 69, "watches/fitbit");

                    tsf.Append(baseline.AddMonths(1).AddMinutes(61), 159, "watches/apple");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(62), 179, "watches/apple");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(63), 169, "watches/fitbit");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.DocumentQuery<User>()
                        .WhereGreaterThan(u => u.Age, 21)
                        .SelectTimeSeries(builder => builder
                            .From("Heartrate")
                            .Between(baseline, baseline.AddMonths(3))
                            .Where(x => x.Tag == "watches/fitbit")
                            .GroupBy(g => g.Hours(1))
                            .ToList());

                    var result = query.ToList();

                    Assert.Equal(1, result.Count);
                    Assert.Equal(3, result[0].Count);

                    var agg = result[0].Results;

                    Assert.Equal(2, agg.Length);

                    Assert.Equal(69, agg[0].Max[0]);
                    Assert.Equal(59, agg[0].Min[0]);
                    Assert.Equal(64, agg[0].Average[0]);

                    Assert.Equal(169, agg[1].Max[0]);
                    Assert.Equal(169, agg[1].Min[0]);
                    Assert.Equal(169, agg[1].Average[0]);
                }
            }
        }

        [Fact]
        public void TimeSeriesDocumentQuery_UsingBuilder_GroupByAndSelect()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today.EnsureUtc();

                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Oren",
                        Age = 35
                    }, "users/ayende");

                    var tsf = session.TimeSeriesFor("users/ayende", "Heartrate");
                    tsf.Append(baseline.AddMinutes(61), 59, "watches/fitbit");
                    tsf.Append(baseline.AddMinutes(62), 79, "watches/apple");
                    tsf.Append(baseline.AddMinutes(63), 69, "watches/fitbit");

                    tsf.Append(baseline.AddMonths(1).AddMinutes(61), 159, "watches/apple");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(62), 179, "watches/apple");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(63), 169, "watches/fitbit");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.DocumentQuery<User>()
                        .WhereGreaterThan(u => u.Age, 21)
                        .SelectTimeSeries(builder => builder
                            .From("Heartrate")
                            .Between(baseline, baseline.AddMonths(3))
                            .Where(x => x.Tag == "watches/fitbit")
                            .GroupBy(g => g.Hours(1))
                            .Select(x => x.Max())
                            .ToList());

                    var result = query.ToList();

                    Assert.Equal(1, result.Count);
                    Assert.Equal(3, result[0].Count);

                    var agg = result[0].Results;

                    Assert.Equal(2, agg.Length);
                    Assert.Equal(69, agg[0].Max[0]);
                    Assert.Equal(169, agg[1].Max[0]);
                }
            }
        }

        [Fact]
        public void TimeSeriesDocumentQuery_UsingBuilder_SelectNoGroupBy()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today.EnsureUtc();

                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Oren",
                        Age = 35
                    }, "users/ayende");

                    var tsf = session.TimeSeriesFor("users/ayende", "Heartrate");
                    tsf.Append(baseline.AddMinutes(61), 59, "watches/fitbit");
                    tsf.Append(baseline.AddMinutes(62), 79, "watches/apple");
                    tsf.Append(baseline.AddMinutes(63), 69, "watches/fitbit");

                    tsf.Append(baseline.AddMonths(1).AddMinutes(61), 159, "watches/apple");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(62), 179, "watches/apple");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(63), 169, "watches/fitbit");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.DocumentQuery<User>()
                        .WhereGreaterThan(u => u.Age, 21)
                        .SelectTimeSeries(builder => builder
                            .From("Heartrate")
                            .Between(baseline, baseline.AddMonths(3))
                            .Where(x => x.Tag == "watches/fitbit")
                            .Select(x => x.Max())
                            .ToList());

                    var result = query.First();

                    Assert.Equal(3, result.Count);
                    Assert.Equal(169, result.Results[0].Max[0]);
                }
            }
        }

        [Fact]
        public void TimeSeriesDocumentQuery_UsingBuilder_WhereOnLoadedTag()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    session.Store(new Person
                    {
                        Age = 25
                    }, "people/1");

                    session.Store(new Watch
                    {
                        Accuracy = 70
                    }, "watches/fitbit");

                    session.Store(new Watch
                    {
                        Accuracy = 90
                    }, "watches/apple");

                    session.Store(new Watch
                    {
                        Accuracy = 180
                    }, "watches/sony");

                    var tsf = session.TimeSeriesFor("people/1", "HeartRate");

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
                    var query = session.Advanced.DocumentQuery<Person>()
                        .WhereGreaterThan(p => p.Age, 21)
                        .SelectTimeSeries(builder => builder
                            .From("Heartrate")
                            .Between(baseline, baseline.AddMonths(2))
                            .LoadByTag<Watch>()
                            .Where((entry, watch) => entry.Value <= watch.Accuracy)
                            .GroupBy(g => g.Months(1))
                            .Select(g => new
                            {
                                Avg = g.Average(),
                                Max = g.Max()
                            })
                            .ToList());

                    var result = query.First();

                    Assert.Equal(4, result.Count);

                    var agg = result.Results;

                    Assert.Equal(2, agg.Length);

                    Assert.Equal(79, agg[0].Max[0]);
                    Assert.Equal(69, agg[0].Average[0]);
                    Assert.Equal(3, agg[0].Count[0]);

                    Assert.Equal(179, agg[1].Max[0]);
                    Assert.Equal(179, agg[1].Average[0]);
                }
            }
        }

        [Fact]
        public void TimeSeriesDocumentQuery_UsingBuilder_GroupByTag()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    session.Store(new Person
                    {
                        Age = 25
                    }, "people/1");

                    session.Store(new Watch
                    {
                        Accuracy = 70
                    }, "watches/fitbit");

                    session.Store(new Watch
                    {
                        Accuracy = 90
                    }, "watches/apple");

                    session.Store(new Watch
                    {
                        Accuracy = 180
                    }, "watches/sony");

                    var tsf = session.TimeSeriesFor("people/1", "HeartRate");

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
                    var query = session.Advanced.DocumentQuery<Person>()
                        .WhereGreaterThan(p => p.Age, 21)
                        .SelectTimeSeries(builder => builder
                            .From("Heartrate")
                            .Between(baseline, baseline.AddMonths(2))
                            .LoadByTag<Watch>()
                            .Where((entry, watch) => entry.Value <= watch.Accuracy)
                            .GroupBy(g => g.Months(1).ByTag<Watch>(w => w.Accuracy))
                            .Select(g => new
                            {
                                Avg = g.Average(),
                                Max = g.Max()
                            })
                            .ToList());

                    var result = query.First();

                    Assert.Equal(4, result.Count);

                    var agg = result.Results;

                    Assert.Equal(3, agg.Length);

                    Assert.Equal(69, agg[0].Max[0]);
                    Assert.Equal(64, agg[0].Average[0]);
                    Assert.Equal(2, agg[0].Count[0]);

                    Assert.Equal(79, agg[1].Max[0]);
                    Assert.Equal(79, agg[1].Average[0]);
                    Assert.Equal(1, agg[1].Count[0]);

                    Assert.Equal(179, agg[2].Max[0]);
                    Assert.Equal(179, agg[2].Average[0]);
                    Assert.Equal(1, agg[2].Count[0]);
                }
            }
        }

        [Fact]
        public void TimeSeriesDocumentQuery_UsingBuilder_GroupByWithInterpolation()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today.EnsureUtc();

                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Oren",
                        Age = 35
                    }, "users/ayende");

                    var tsf = session.TimeSeriesFor("users/ayende", "Heartrate");

                    tsf.Append(baseline, 0);
                    tsf.Append(baseline.AddMinutes(10), 10);
                    tsf.Append(baseline.AddMinutes(50), 50);

                    tsf.Append(baseline.AddHours(2), 120);
                    tsf.Append(baseline.AddHours(2).AddMinutes(15), 145);
                    tsf.Append(baseline.AddHours(2).AddMinutes(50), 170);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.DocumentQuery<User>()
                        .WhereGreaterThan(u => u.Age, 21)
                        .SelectTimeSeries(builder => builder
                            .From("Heartrate")
                            .GroupBy(g => g
                                .Hours(1)
                                .WithOptions(new TimeSeriesAggregationOptions
                                {
                                    Interpolation = InterpolationType.Linear
                                }))
                            .Select(x => x.Max())
                            .ToList());

                    var result = query.First();

                    Assert.Equal(9, result.Count);

                    Assert.Equal(50, result.Results[0].Max[0]);
                    Assert.Equal(110, result.Results[1].Max[0]);
                    Assert.Equal(170, result.Results[2].Max[0]);
                }
            }
        }

        [Fact]
        public void TimeSeriesDocumentQuery_UsingBuilder_FromLast()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today.EnsureUtc().AddDays(-7);
                var id = "people/1";
                var totalMinutes = TimeSpan.FromDays(3).TotalMinutes;

                using (var session = store.OpenSession())
                {
                    session.Store(new Person
                    {
                        Name = "Oren",
                    }, id);

                    var tsf = session.TimeSeriesFor(id, "HeartRate");

                    for (int i = 0; i <= totalMinutes; i++)
                    {
                        tsf.Append(baseline.AddMinutes(i), i, "watches/fitbit");
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.DocumentQuery<Person>()
                        .WhereEquals(p => p.Id, id)
                        .SelectTimeSeries(builder => builder
                            .From("HeartRate")
                            .FromLast(g => g.Hours(12))
                            .ToList());

                    var result = query.First();

                    var expectedInitialTimestamp = baseline.AddDays(3).AddHours(-12);
                    var expectedInitialValueValue = totalMinutes - TimeSpan.FromHours(12).TotalMinutes;
                    var expectedCount = totalMinutes - expectedInitialValueValue + 1;

                    Assert.Equal(expectedCount, result.Count);

                    for (int i = 0; i < expectedCount; i++)
                    {
                        Assert.Equal(expectedInitialValueValue + i, result.Results[i].Value);
                        Assert.Equal(expectedInitialTimestamp.AddMinutes(i), result.Results[i].Timestamp);
                    }
                }
            }
        }

        [Fact]
        public void TimeSeriesDocumentQuery_UsingBuilder_FromFirst()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today.EnsureUtc().AddDays(-7);
                var id = "people/1";
                var totalMinutes = TimeSpan.FromDays(3).TotalMinutes;

                using (var session = store.OpenSession())
                {
                    session.Store(new Person
                    {
                        Name = "Oren",
                    }, id);

                    var tsf = session.TimeSeriesFor(id, "HeartRate");

                    for (int i = 0; i <= totalMinutes; i++)
                    {
                        tsf.Append(baseline.AddMinutes(i), i, "watches/fitbit");
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var name = "HeartRate";

                    var query = session.Advanced.DocumentQuery<Person>()
                        .WhereEquals(p => p.Id, id)
                        .SelectTimeSeries(builder => builder
                            .From(name)
                            .FromFirst(g => g.Seconds(90))
                            .GroupBy(g => g.Seconds(10))
                            .Select(x => x.Average())
                            .ToList());

                    var result = query.First();

                    Assert.Equal(2, result.Count);
                    Assert.Equal(2, result.Results.Length);

                    Assert.Equal(baseline, result.Results[0].From, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(result.Results[0].From.AddSeconds(10), result.Results[0].To, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(baseline.AddMinutes(1), result.Results[1].From, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(result.Results[1].From.AddSeconds(10), result.Results[1].To, RavenTestHelper.DateTimeComparer.Instance);
                }
            }
        }

        [Fact]
        public void ShouldThrowOnUsingLastAndBetweenInTheSameTimeSeriesDocumentQuery()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today.EnsureUtc().AddDays(-7);
                var id = "people/1";
                var totalMinutes = TimeSpan.FromDays(3).TotalMinutes;

                using (var session = store.OpenSession())
                {
                    session.Store(new Person
                    {
                        Name = "Oren",
                    }, id);

                    session.TimeSeriesFor(id, "HeartRate").Append(baseline, 60, "watches/fitbit");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var ex = Assert.Throws<InvalidOperationException>(() => session.Advanced.DocumentQuery<Person>()
                        .WhereEquals(p => p.Id, id)
                        .SelectTimeSeries(builder => builder
                            .From("HeartRate")
                            .Between(baseline, baseline.AddDays(1))
                            .FromLast(g => g.Hours(6))
                            .GroupBy(g => g.Hours(1))
                            .Select(x => new { Avg = x.Average() })
                            .ToList()));

                    Assert.NotNull(ex.InnerException);
                    Assert.Contains("Cannot use 'FromLast' when From/To dates are provided to the Time Series query function", ex.InnerException.Message);
                }
            }
        }

        [Fact]
        public void TimeSeriesDocumentQuery_UsingBuilder_Offset()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    var id = $"people/1";

                    session.Store(new Person
                    {
                        Name = "Oren",
                        Age = 30,
                    }, id);

                    var tsf = session.TimeSeriesFor(id, "HeartRate");

                    tsf.Append(baseline.AddMinutes(1), new[] { 59d }, "watches/fitbit");
                    tsf.Append(baseline.AddMinutes(2), new[] { 79d }, "watches/apple");
                    tsf.Append(baseline.AddMinutes(3), new[] { 69d }, "watches/sony");

                    tsf.Append(baseline.AddHours(1).AddMinutes(1), new[] { 159d }, "watches/fitbit");
                    tsf.Append(baseline.AddHours(1).AddMinutes(2), new[] { 179d }, "watches/apple");
                    tsf.Append(baseline.AddHours(1).AddMinutes(3), new[] { 169d }, "watches/sony");

                    tsf.Append(baseline.AddHours(2).AddMinutes(1), new[] { 259d }, "watches/fitbit");
                    tsf.Append(baseline.AddHours(2).AddMinutes(2), new[] { 279d }, "watches/apple");
                    tsf.Append(baseline.AddHours(2).AddMinutes(3), new[] { 269d }, "watches/sony");

                    tsf.Append(baseline.AddMonths(1).AddMinutes(1), new[] { 359d }, "watches/apple");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(2), new[] { 379d }, "watches/sony");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(3), new[] { 369d }, "watches/fitbit");

                    tsf.Append(baseline.AddMonths(6).AddHours(1).AddMinutes(1), new[] { 459d }, "watches/apple");
                    tsf.Append(baseline.AddMonths(6).AddHours(1).AddMinutes(2), new[] { 479d }, "watches/sony");
                    tsf.Append(baseline.AddMonths(6).AddHours(1).AddMinutes(3), new[] { 469d }, "watches/fitbit");

                    tsf.Append(baseline.AddMonths(6).AddHours(2).AddMinutes(1), new[] { 559d }, "watches/apple");
                    tsf.Append(baseline.AddMonths(6).AddHours(2).AddMinutes(2), new[] { 579d }, "watches/sony");
                    tsf.Append(baseline.AddMonths(6).AddHours(2).AddMinutes(3), new[] { 569d }, "watches/fitbit");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var offset = TimeSpan.FromHours(2);

                    var query = session.Advanced.DocumentQuery<Person>()
                        .SelectTimeSeries(builder => builder
                            .From("HeartRate")
                            .Between(baseline, baseline.AddMonths(6))
                            .GroupBy(g => g.Hours(1))
                            .Select(ts => new
                            {
                                Max = ts.Max(),
                                Min = ts.Min()
                            })
                            .Offset(offset)
                            .ToList());

                    var agg = query.First();

                    Assert.Equal(12, agg.Count);

                    Assert.Equal(4, agg.Results.Length);

                    var rangeAggregation = agg.Results[0];

                    Assert.Equal(59, rangeAggregation.Min[0]);
                    Assert.Equal(79, rangeAggregation.Max[0]);

                    var expectedFrom = baseline.Add(offset);
                    var expectedTo = expectedFrom.AddHours(1);

                    Assert.Equal(expectedFrom, rangeAggregation.From, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(expectedTo, rangeAggregation.To, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(DateTimeKind.Unspecified, rangeAggregation.From.Kind);
                    Assert.Equal(DateTimeKind.Unspecified, rangeAggregation.To.Kind);

                    rangeAggregation = agg.Results[1];

                    Assert.Equal(159, rangeAggregation.Min[0]);
                    Assert.Equal(179, rangeAggregation.Max[0]);

                    expectedFrom = baseline.AddHours(1).Add(offset);
                    expectedTo = expectedFrom.AddHours(1);

                    Assert.Equal(expectedFrom, rangeAggregation.From, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(expectedTo, rangeAggregation.To, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(DateTimeKind.Unspecified, rangeAggregation.From.Kind);
                    Assert.Equal(DateTimeKind.Unspecified, rangeAggregation.To.Kind);

                    rangeAggregation = agg.Results[2];

                    Assert.Equal(259, rangeAggregation.Min[0]);
                    Assert.Equal(279, rangeAggregation.Max[0]);

                    expectedFrom = baseline.AddHours(2).Add(offset);
                    expectedTo = expectedFrom.AddHours(1);

                    Assert.Equal(expectedFrom, rangeAggregation.From, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(expectedTo, rangeAggregation.To, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(DateTimeKind.Unspecified, rangeAggregation.From.Kind);
                    Assert.Equal(DateTimeKind.Unspecified, rangeAggregation.To.Kind);

                    rangeAggregation = agg.Results[3];

                    Assert.Equal(359, rangeAggregation.Min[0]);
                    Assert.Equal(379, rangeAggregation.Max[0]);

                    expectedFrom = baseline.AddMonths(1).Add(offset);
                    expectedTo = expectedFrom.AddHours(1);

                    Assert.Equal(expectedFrom, rangeAggregation.From, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(expectedTo, rangeAggregation.To, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(DateTimeKind.Unspecified, rangeAggregation.From.Kind);
                    Assert.Equal(DateTimeKind.Unspecified, rangeAggregation.To.Kind);
                }
            }
        }

        [Fact]
        public void TimeSeriesDocumentQuery_UsingBuilder_Scale()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;
                var id = "people/1";
                var name = "HeartRate";
                var totalMinutes = TimeSpan.FromDays(3).TotalMinutes;

                using (var session = store.OpenSession())
                {
                    session.Store(new Person
                    {
                        Name = "Oren",
                    }, id);

                    var tsf = session.TimeSeriesFor(id, name);

                    for (int i = 0; i < totalMinutes; i++)
                    {
                        tsf.Append(baseline.AddMinutes(i), i, "watches/fitbit");
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var scale = 0.01;

                    var query = session.Advanced.DocumentQuery<Person>()
                        .WhereEquals(p => p.Id, id)
                        .SelectTimeSeries(builder => builder
                            .From("HeartRate")
                            .Between(baseline, baseline.AddDays(3))
                            .GroupBy(g => g.Hours(1))
                            .Select(x => new { Max = x.Max(), Min = x.Min() })
                            .Scale(scale)
                            .ToList());

                    var result = query.First();

                    var expectedTotalCount = TimeSpan.FromDays(3).TotalMinutes;
                    var expectedBucketCount = TimeSpan.FromDays(3).TotalHours;

                    Assert.Equal(expectedTotalCount, result.Count);
                    Assert.Equal(expectedBucketCount, result.Results.Length);

                    var tolerance = 0.0000001;

                    for (int i = 0; i < result.Results.Length; i++)
                    {
                        Assert.Equal(60, result.Results[i].Count[0]);

                        var min = result.Results[i].Min[0];
                        var expectedMin = scale * i * 60;
                        Assert.True(Math.Abs(expectedMin - min) < tolerance);

                        var max = result.Results[i].Max[0];
                        var expectedMax = scale * ((i + 1) * 60 - 1);

                        Assert.True(Math.Abs(expectedMax - max) < tolerance);
                    }
                }
            }
        }

        [Fact]
        public void TimeSeriesDocumentQuery_UsingBuilder_ScaleAndOffset()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday.EnsureUtc();
                var id = "people/1";
                var name = "HeartRate";
                var totalHours = TimeSpan.FromDays(3).TotalHours;

                using (var session = store.OpenSession())
                {
                    session.Store(new Person
                    {
                        Name = "Oren",
                    }, id);

                    var tsf = session.TimeSeriesFor(id, name);

                    for (int i = 0; i < totalHours; i++)
                    {
                        tsf.Append(baseline.AddHours(i), i, "watches/fitbit");
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var offset = TimeZoneInfo.Local.BaseUtcOffset;
                    var scale = 0.001;

                    var query = session.Advanced.DocumentQuery<Person>()
                        .WhereEquals(p => p.Id, id)
                        .SelectTimeSeries(builder => builder
                            .From(name)
                            .Between(baseline, baseline.AddDays(3))
                            .Scale(scale)
                            .Offset(offset)
                            .ToList());

                    var result = query.First();

                    var expectedTotalCount = TimeSpan.FromDays(3).TotalHours;

                    Assert.Equal(expectedTotalCount, result.Count);

                    var baselineWithOffset = baseline.Add(offset);

                    for (int i = 0; i < result.Results.Length; i++)
                    {
                        var expectedTimestamp = baselineWithOffset.AddHours(i);
                        Assert.Equal(expectedTimestamp, result.Results[i].Timestamp);

                        var expectedVal = i * scale;
                        var val = result.Results[i].Value;

                        Assert.True(expectedVal.AlmostEquals(val));
                        Assert.Equal(expectedVal, result.Results[i].Value);
                    }
                }
            }
        }

        [Fact]
        public async Task TimeSeriesAsyncDocumentQuery_UsingBuilder()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today.EnsureUtc();

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User
                    {
                        Name = "Oren",
                        Age = 35
                    }, "users/ayende");

                    var tsf = session.TimeSeriesFor("users/ayende", "Heartrate");
                    tsf.Append(baseline.AddMinutes(61), 59, "watches/fitbit");
                    tsf.Append(baseline.AddMinutes(62), 79, "watches/apple");
                    tsf.Append(baseline.AddMinutes(63), 69, "watches/fitbit");

                    tsf.Append(baseline.AddMonths(1).AddMinutes(61), 159, "watches/apple");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(62), 179, "watches/apple");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(63), 169, "watches/fitbit");

                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var query = session.Advanced.AsyncDocumentQuery<User>()
                        .WhereGreaterThan(u => u.Age, 21)
                        .SelectTimeSeries(builder => builder
                            .From("Heartrate")
                            .Between(baseline, baseline.AddMonths(3))
                            .Where(x => x.Tag == "watches/fitbit")
                            .GroupBy(g => g.Hours(1))
                            .Select(x => x.Max())
                            .ToList());

                    var result = await query.ToListAsync();

                    Assert.Equal(1, result.Count);
                    Assert.Equal(3, result[0].Count);

                    var agg = result[0].Results;

                    Assert.Equal(2, agg.Length);
                    Assert.Equal(69, agg[0].Max[0]);
                    Assert.Equal(169, agg[1].Max[0]);
                }
            }
        }
    }
}
