using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Newtonsoft.Json.Linq;
using Raven.Client.Documents;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Queries.TimeSeries;
using Raven.Tests.Core.Utils.Entities;
using Sparrow;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client.TimeSeries.Query
{
    public class TimeSeriesGroupByTag : RavenTestBase
    {
        public TimeSeriesGroupByTag(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanGroupByTagRql()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, "users/ayende");

                    var tsf = session.TimeSeriesFor("users/ayende", "Heartrate");
                    tsf.Append(baseline.AddMinutes(61), new[] { 59d }, "watches/fitbit");
                    tsf.Append(baseline.AddMinutes(62), new[] { 79d }, "watches/fitbit");
                    tsf.Append(baseline.AddMinutes(63), new[] { 69d }, "watches/apple");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesAggregationResult>(@"
    declare timeseries out(u) 
    {
        from u.Heartrate between $start and $end
        group by 1h, tag
        select min(), max(), first(), last()
    }
    from @all_docs as u
    where id() == 'users/ayende'
    select out(u)
")
                        .AddParameter("start", baseline.EnsureUtc())
                        .AddParameter("end", baseline.AddDays(1).EnsureUtc());

                    var agg = query.First();
                    
                    Assert.Equal(3, agg.Count);

                    Assert.Equal(2, agg.Results.Length);

                    var val1 = agg.Results[0];

                    Assert.Equal(59, val1.First[0]);
                    Assert.Equal(59, val1.Min[0]);

                    Assert.Equal(79, val1.Last[0]);
                    Assert.Equal(79, val1.Max[0]);

                    Assert.Equal(baseline.AddMinutes(60), val1.From, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(baseline.AddMinutes(120), val1.To, RavenTestHelper.DateTimeComparer.Instance);

                    var val2 = agg.Results[1];

                    Assert.Equal(69, val2.First[0]);
                    Assert.Equal(69, val2.Min[0]);

                    Assert.Equal(69, val2.Last[0]);
                    Assert.Equal(69, val2.Max[0]);

                    Assert.Equal(baseline.AddMinutes(60), val2.From, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(baseline.AddMinutes(120), val2.To, RavenTestHelper.DateTimeComparer.Instance);
                }
            }
        }

        [Fact]
        public void CanGroupByTagLinq()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Oren" }, "users/ayende");

                    var tsf = session.TimeSeriesFor("users/ayende", "Heartrate");
                    tsf.Append(baseline.AddMinutes(61), new[] { 59d }, "watches/fitbit");
                    tsf.Append(baseline.AddMinutes(62), new[] { 79d }, "watches/fitbit");
                    tsf.Append(baseline.AddMinutes(63), new[] { 69d }, "watches/apple");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Query<User>()
                        .Select(u => RavenQuery.TimeSeries(u, "Heartrate",baseline.EnsureUtc(), baseline.AddDays(1).EnsureUtc())
                            .GroupBy(g => g
                                    .Hours(1)
                                    .ByTag()
                                   )
                            .Select(g => new
                            {
                                First = g.First(),
                                Max = g.Max(),
                                Min = g.Min(),
                                Last = g.Last(),
                            })
                            .ToList());

                    var agg = query.First();
                    
                    Assert.Equal(3, agg.Count);

                    Assert.Equal(2, agg.Results.Length);

                    var val1 = agg.Results[0];

                    Assert.Equal(59, val1.First[0]);
                    Assert.Equal(59, val1.Min[0]);

                    Assert.Equal(79, val1.Last[0]);
                    Assert.Equal(79, val1.Max[0]);

                    Assert.Equal(baseline.AddMinutes(60), val1.From, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(baseline.AddMinutes(120), val1.To, RavenTestHelper.DateTimeComparer.Instance);

                    var val2 = agg.Results[1];

                    Assert.Equal(69, val2.First[0]);
                    Assert.Equal(69, val2.Min[0]);

                    Assert.Equal(69, val2.Last[0]);
                    Assert.Equal(69, val2.Max[0]);

                    Assert.Equal(baseline.AddMinutes(60), val2.From, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(baseline.AddMinutes(120), val2.To, RavenTestHelper.DateTimeComparer.Instance);
                }
            }
        }

        [Fact]
        public void CanGroupByLoadedTagRql()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = PopulateCanGroupByLoadedTag(store);

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesAggregationResult>(@"
    declare timeseries out(u) 
    {
        from u.Heartrate between $start and $end
        load Tag as watch
        group by 1month, watch.Accuracy
        select avg(), max()
    }
    from people as u
    select out(u)
")
                        .AddParameter("start", baseline.EnsureUtc())
                        .AddParameter("end", baseline.AddMonths(2).EnsureUtc());

                    var result = query.First();

                    Assert.Equal(6, result.Count);

                    var agg = result.Results;

                    Assert.Equal(3, agg.Length);

                    var monthBaseline = new DateTime(baseline.Year, baseline.Month, 1);

                    var val1 = agg[0];
                    Assert.Equal(10.0m, val1.Key);
                    Assert.Equal(monthBaseline, val1.From, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(monthBaseline.AddMonths(1), val1.To, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(3, val1.Count[0]);

                    var val2 = agg[1];
                    Assert.Equal(10.0m, val2.Key);
                    Assert.Equal(monthBaseline.AddMonths(1), val2.From, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(monthBaseline.AddMonths(2), val2.To, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(2, val2.Count[0]);

                    var val3 = agg[2];
                    Assert.Equal(5.0m, val3.Key);
                    Assert.Equal(monthBaseline.AddMonths(1), val3.From, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(monthBaseline.AddMonths(2), val3.To, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(1, val3.Count[0]);
                }
            }
        }

        [Fact]
        public void CanGroupByNull()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = PopulateCanGroupByLoadedTag(store);

                using (var session = store.OpenSession())
                {
                    var query = session.Query<TimeSeriesLinqQuery.Person>()
                        .Select(u => RavenQuery.TimeSeries(u, "Heartrate", baseline.EnsureUtc(), baseline.AddMonths(2).EnsureUtc())
                            .GroupBy(g => g
                                .Months(1)
                                .ByTag<TimeSeriesLinqQuery.Watch>(w => w.Year)
                            )
                            .Select(g => new {Average = g.Average(), Max = g.Max()})
                            .ToList());

                    var result = query.First();

                    Assert.Equal(6, result.Count);

                    var agg = result.Results;

                    Assert.Equal(3, agg.Length);

                    var monthBaseline = new DateTime(baseline.Year, baseline.Month, 1);

                    var val1 = agg[0];
                    Assert.Equal(null, val1.Key);
                    Assert.Equal(monthBaseline, val1.From, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(monthBaseline.AddMonths(1), val1.To, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(3, val1.Count[0]);

                    var val2 = agg[1];
                    Assert.Equal(null, val2.Key);
                    Assert.Equal(monthBaseline.AddMonths(1), val2.From, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(monthBaseline.AddMonths(2), val2.To, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(2, val2.Count[0]);

                    var val3 = agg[2];
                    Assert.Equal(0L, val3.Key);
                    Assert.Equal(monthBaseline.AddMonths(1), val3.From, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(monthBaseline.AddMonths(2), val3.To, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(1, val3.Count[0]);
                }
            }
        }

        [Fact]
        public void CanGroupByDocument()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = PopulateCanGroupByLoadedTag(store);

                using (var session = store.OpenSession())
                {
                    var query = session.Query<TimeSeriesLinqQuery.Person>()
                        .Select(u => RavenQuery.TimeSeries(u, "Heartrate", baseline.EnsureUtc(), baseline.AddMonths(2).EnsureUtc())
                            .GroupBy(g => g
                                .Months(1)
                                .ByTag<TimeSeriesLinqQuery.Watch>(w => w)
                            )
                            .Select(g => new {Average = g.Average(), Max = g.Max()})
                            .ToList());

                    var result = query.First();

                    Assert.Equal(6, result.Count);

                    var agg = result.Results;

                    Assert.Equal(5, agg.Length);

                    var monthBaseline = new DateTime(baseline.Year, baseline.Month, 1);

                    var val1 = agg[0];
                    Assert.Equal("watches/fitbit", val1.Key);
                    Assert.Equal(monthBaseline, val1.From, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(monthBaseline.AddMonths(1), val1.To, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(2, val1.Count[0]);

                    var val2 = agg[1];
                    Assert.Equal("watches/apple", val2.Key);
                    Assert.Equal(monthBaseline, val2.From, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(monthBaseline.AddMonths(1), val2.To, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(1, val2.Count[0]);

                    var val3 = agg[2];
                    Assert.Equal("watches/apple", val3.Key);
                    Assert.Equal(monthBaseline.AddMonths(1), val3.From, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(monthBaseline.AddMonths(2), val3.To, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(1, val3.Count[0]);

                    var val4 = agg[3];
                    Assert.Equal("watches/sony", val4.Key);
                    Assert.Equal(monthBaseline.AddMonths(1), val4.From, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(monthBaseline.AddMonths(2), val4.To, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(1, val4.Count[0]);

                    var val5 = agg[4];
                    Assert.Equal("watches/fitbit", val5.Key);
                    Assert.Equal(monthBaseline.AddMonths(1), val5.From, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(monthBaseline.AddMonths(2), val5.To, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(1, val5.Count[0]);
                }
            }
        }

        [Fact]
        public void CanGroupByArray()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = PopulateCanGroupByLoadedTag(store);

                using (var session = store.OpenSession())
                {
                    var query = session.Query<TimeSeriesLinqQuery.Person>()
                        .Select(u => RavenQuery.TimeSeries(u, "Heartrate", baseline.EnsureUtc(), baseline.AddMonths(2).EnsureUtc())
                            .GroupBy(g => g
                                .Months(1)
                                .ByTag<TimeSeriesLinqQuery.Watch>(w => w.Prizes)
                            )
                            .Select(g => new {Average = g.Average(), Max = g.Max()})
                            .ToList());

                    var result = query.First();

                    Assert.Equal(6, result.Count);

                    var agg = result.Results;

                    Assert.Equal(4, agg.Length);

                    var monthBaseline = new DateTime(baseline.Year, baseline.Month, 1);

                    var val1 = agg[0];
                    Assert.Equal(new JArray("2012","2013"), val1.Key);
                    Assert.Equal(monthBaseline, val1.From, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(monthBaseline.AddMonths(1), val1.To, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(2, val1.Count[0]);

                    var val2 = agg[1];
                    Assert.Equal(new JArray("2012"), val2.Key);
                    Assert.Equal(monthBaseline, val2.From, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(monthBaseline.AddMonths(1), val2.To, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(1, val2.Count[0]);

                    var val3 = agg[2];
                    Assert.Equal(new JArray("2012"), val3.Key);
                    Assert.Equal(monthBaseline.AddMonths(1), val3.From, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(monthBaseline.AddMonths(2), val3.To, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(2, val3.Count[0]);

                    var val4 = agg[3];
                    Assert.Equal(new JArray("2012","2013"), val4.Key);
                    Assert.Equal(monthBaseline.AddMonths(1), val4.From, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(monthBaseline.AddMonths(2), val4.To, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(1, val4.Count[0]);
                }
            }
        }

        [Fact]
        public void CanGroupByLoadedTagLinq()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = PopulateCanGroupByLoadedTag(store);

                using (var session = store.OpenSession())
                {
                    var query = session.Query<TimeSeriesLinqQuery.Person>()
                        .Select(u => RavenQuery.TimeSeries(u, "Heartrate", baseline.EnsureUtc(), baseline.AddMonths(2).EnsureUtc())
                            .GroupBy(g => g
                                .Months(1)
                                .ByTag<TimeSeriesLinqQuery.Watch>(w => w.Accuracy)
                            )
                            .Select(g => new {Average = g.Average(), Max = g.Max()})
                            .ToList());

                    var result = query.First();

                    Assert.Equal(6, result.Count);

                    var agg = result.Results;

                    Assert.Equal(3, agg.Length);

                    var monthBaseline = new DateTime(baseline.Year, baseline.Month, 1);

                    var val1 = agg[0];
                    Assert.Equal(10.0m, val1.Key);
                    Assert.Equal(monthBaseline, val1.From, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(monthBaseline.AddMonths(1), val1.To, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(3, val1.Count[0]);

                    var val2 = agg[1];
                    Assert.Equal(10.0m, val2.Key);
                    Assert.Equal(monthBaseline.AddMonths(1), val2.From, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(monthBaseline.AddMonths(2), val2.To, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(2, val2.Count[0]);

                    var val3 = agg[2];
                    Assert.Equal(5.0m, val3.Key);
                    Assert.Equal(monthBaseline.AddMonths(1), val3.From, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(monthBaseline.AddMonths(2), val3.To, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(1, val3.Count[0]);
                }
            }
        }

        [Fact]
        public void CanGroupByLoadedTagWithTagFilter()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = PopulateCanGroupByLoadedTag(store);

                using (var session = store.OpenSession())
                {
                    var query = session.Query<TimeSeriesLinqQuery.Person>()
                        .Select(u => RavenQuery.TimeSeries(u, "Heartrate", baseline.EnsureUtc(), baseline.AddMonths(2).EnsureUtc())
                            .LoadByTag<TimeSeriesLinqQuery.Watch>().Where((entry, watch) => watch.Manufacturer != "Sony")
                            .GroupBy(g => g
                                .Months(1)
                                .ByTag<TimeSeriesLinqQuery.Watch>(w => w.Accuracy)
                            )
                            .Select(g => new {Average = g.Average(), Max = g.Max()})
                            .ToList());

                    var result = query.First();

                    Assert.Equal(5, result.Count);

                    var agg = result.Results;

                    Assert.Equal(2, agg.Length);

                    var monthBaseline = new DateTime(baseline.Year, baseline.Month, 1);

                    var val1 = agg[0];
                    Assert.Equal(10.0m, val1.Key);
                    Assert.Equal(monthBaseline, val1.From, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(monthBaseline.AddMonths(1), val1.To, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(3, val1.Count[0]);

                    var val2 = agg[1];
                    Assert.Equal(10.0m, val2.Key);
                    Assert.Equal(monthBaseline.AddMonths(1), val2.From, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(monthBaseline.AddMonths(2), val2.To, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(2, val2.Count[0]);
                }
            }
        }

        [Fact]
        public void CanGroupByTagWithInterpolationRql()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = PopulateCanGroupByTagWithInterpolation(store);

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesAggregationResult>(@"
    declare timeseries out(u) 
    {
        from u.Heartrate between $start and $end
        group by 1h, tag
        with interpolation(linear)
        select avg(), max()
    }
    from people as u
    select out(u)
")
                        .AddParameter("start", baseline.EnsureUtc())
                        .AddParameter("end", baseline.AddMonths(2).EnsureUtc());

                    var result = query.First();

                    Assert.Equal(32 * 3, result.Count);

                    var agg = result.Results;

                    Assert.Equal(32 * 3, agg.Length);

                    var groups = agg.GroupBy(x => x.Key);

                    foreach (var group in groups)
                    {
                        var key = group.Key;
                        var value = group.OrderBy(x => x.From).ToArray();
                        Assert.Equal(32, value.Length);
                        for (int i = 0; i < value.Length; i++)
                        {
                            var val = value[i];
                            switch (key)
                            {
                                case "watches/fitbit":
                                    Assert.Equal(i * 10, val.Average[0]);
                                    break;
                                case "watches/apple":
                                    Assert.Equal(i * 100, val.Average[0]);
                                    break;
                                case "watches/sony":
                                    Assert.Equal(i * 1000, val.Average[0]);
                                    break;
                                default:
                                    throw new ArgumentException();
                            }
                            Assert.Equal(baseline.AddHours(i), val.From, RavenTestHelper.DateTimeComparer.Instance);
                            Assert.Equal(baseline.AddHours(i + 1), val.To, RavenTestHelper.DateTimeComparer.Instance);
                        }
                    }
                }
            }
        }

        [Fact]
        public void CanGroupByTagWithInterpolationLinq()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = PopulateCanGroupByTagWithInterpolation(store);

                using (var session = store.OpenSession())
                {
                    var query = session.Query<TimeSeriesLinqQuery.Person>()
                        .Select(u => RavenQuery.TimeSeries(u, "Heartrate",baseline.EnsureUtc(), baseline.AddMonths(2).EnsureUtc())
                            //    .LoadByTag<TimeSeriesLinqQuery.Watch>().Where((entry, watch) => true)
                            .GroupBy(g => g
                                .Hours(1)
                                .ByTag()
                                .WithOptions(new TimeSeriesAggregationOptions
                                {
                                    Interpolation = InterpolationType.Linear
                                }))
                            .Select(g => new
                            {
                                Max = g.Max(),
                                Average = g.Average(),
                            })
                            .ToList());


                    var result = query.First();

                    Assert.Equal(32 * 3, result.Count);

                    var agg = result.Results;

                    Assert.Equal(32 * 3, agg.Length);

                    var groups = agg.GroupBy(x => x.Key);

                    foreach (var group in groups)
                    {
                        var key = group.Key;
                        var value = group.OrderBy(x => x.From).ToArray();
                        Assert.Equal(32, value.Length);
                        for (int i = 0; i < value.Length; i++)
                        {
                            var val = value[i];
                            switch (key)
                            {
                                case "watches/fitbit":
                                    Assert.Equal(i * 10, val.Average[0]);
                                    break;
                                case "watches/apple":
                                    Assert.Equal(i * 100, val.Average[0]);
                                    break;
                                case "watches/sony":
                                    Assert.Equal(i * 1000, val.Average[0]);
                                    break;
                                default:
                                    throw new ArgumentException();
                            }
                            Assert.Equal(baseline.AddHours(i), val.From, RavenTestHelper.DateTimeComparer.Instance);
                            Assert.Equal(baseline.AddHours(i + 1), val.To, RavenTestHelper.DateTimeComparer.Instance);
                        }
                    }
                }
            }
        }


        [Fact]
        public void CanStreamGroupByTagWithInterpolation()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = PopulateCanGroupByTagWithInterpolation(store);

                using (var session = store.OpenSession())
                {
                    var query = session.Query<TimeSeriesLinqQuery.Person>()
                        .Select(u => RavenQuery.TimeSeries(u, "Heartrate",baseline.EnsureUtc(), baseline.AddMonths(2).EnsureUtc())
                            .GroupBy(g => g
                                .Hours(1)
                                .ByTag()
                                .WithOptions(new TimeSeriesAggregationOptions
                                {
                                    Interpolation = InterpolationType.Linear
                                }))
                            .Select(g => new
                            {
                                Max = g.Max(),
                                Average = g.Average(),
                            })
                            .ToList());

                    var dic = new Dictionary<string, List<TimeSeriesRangeAggregation>>();
                    using (var docStream = session.Advanced.Stream(query))
                    {
                        while (docStream.MoveNext())
                        {
                            using (var entryStream = docStream.Current.Result.Stream)
                            {
                                while (entryStream.MoveNext())
                                {
                                    var current = entryStream.Current;

                                    dic.TryAdd(current.Key.ToString(), new List<TimeSeriesRangeAggregation>());
                                    dic[current.Key.ToString()].Add(current);
                                }
                            }
                        }
                    }

                    Assert.Equal(3, dic.Count);

                    foreach (var group in dic)
                    {
                        var key = group.Key;
                        var value = group.Value.OrderBy(x => x.From).ToArray();
                        Assert.Equal(32, value.Length);
                        for (int i = 0; i < value.Length; i++)
                        {
                            var val = value[i];
                            switch (key)
                            {
                                case "watches/fitbit":
                                    Assert.Equal(i * 10, val.Average[0]);
                                    break;
                                case "watches/apple":
                                    Assert.Equal(i * 100, val.Average[0]);
                                    break;
                                case "watches/sony":
                                    Assert.Equal(i * 1000, val.Average[0]);
                                    break;
                                default:
                                    throw new ArgumentException();
                            }
                            Assert.Equal(baseline.AddHours(i), val.From, RavenTestHelper.DateTimeComparer.Instance);
                            Assert.Equal(baseline.AddHours(i + 1), val.To, RavenTestHelper.DateTimeComparer.Instance);
                        }
                    }
                }
            }
        }

        private static DateTime PopulateCanGroupByLoadedTag(DocumentStore store)
        {
            var baseline = DateTime.Today.ToUniversalTime();

            using (var session = store.OpenSession())
            {
                session.Store(new TimeSeriesLinqQuery.Person {Age = 25}, "people/1");

                session.Store(new TimeSeriesLinqQuery.Watch {Manufacturer = "Fitbit", Accuracy = 10, Prizes = new List<string>{"2012","2013"}}, "watches/fitbit");

                session.Store(new TimeSeriesLinqQuery.Watch {Manufacturer = "Apple", Accuracy = 10, Prizes = new List<string>{"2012"}}, "watches/apple");

                session.Store(new TimeSeriesLinqQuery.Watch {Manufacturer = "Sony", Accuracy = 5, Year = 0, Prizes = new List<string>{"2012"}}, "watches/sony");

                var tsf = session.TimeSeriesFor("people/1", "HeartRate");

                tsf.Append(baseline.AddMinutes(61), new[] {59d}, "watches/fitbit");
                tsf.Append(baseline.AddMinutes(62), new[] {79d}, "watches/apple");
                tsf.Append(baseline.AddMinutes(63), new[] {69d}, "watches/fitbit");

                tsf.Append(baseline.AddMonths(1).AddMinutes(61), new[] {159d}, "watches/apple");
                tsf.Append(baseline.AddMonths(1).AddMinutes(62), new[] {179d}, "watches/sony");
                tsf.Append(baseline.AddMonths(1).AddMinutes(63), new[] {169d}, "watches/fitbit");


                session.SaveChanges();
            }

            return baseline;
        }

        private static DateTime PopulateCanGroupByTagWithInterpolation(DocumentStore store)
        {
            var baseline = DateTime.Today;

            using (var session = store.OpenSession())
            {
                session.Store(new TimeSeriesLinqQuery.Person {Age = 25}, "people/1");

                var tsf = session.TimeSeriesFor("people/1", "HeartRate");

                tsf.Append(baseline.AddMinutes(1), new[] {0d}, "watches/fitbit");
                tsf.Append(baseline.AddMinutes(2), new[] {0d}, "watches/apple");
                tsf.Append(baseline.AddMinutes(3), new[] {0d}, "watches/sony");

                for (int i = 1; i <= 30; i++)
                {
                    switch (i % 3)
                    {
                        case 1:
                            tsf.Append(baseline.AddHours(i).AddMinutes(1), new[] {10d * i}, "watches/fitbit");
                            break;
                        case 2:
                            tsf.Append(baseline.AddHours(i).AddMinutes(2), new[] {100d * i}, "watches/apple");
                            break;
                        case 0:
                            tsf.Append(baseline.AddHours(i).AddMinutes(3), new[] {1000d * i}, "watches/sony");
                            break;
                    }
                }

                tsf.Append(baseline.AddHours(31).AddMinutes(1), new[] {310d}, "watches/fitbit");
                tsf.Append(baseline.AddHours(31).AddMinutes(2), new[] {3100d}, "watches/apple");
                tsf.Append(baseline.AddHours(31).AddMinutes(3), new[] {31000d}, "watches/sony");

                session.SaveChanges();
            }

            return baseline;
        }
    }
}
