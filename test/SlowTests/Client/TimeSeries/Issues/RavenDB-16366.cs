using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Queries.TimeSeries;
using Raven.Client.Documents.Session.TimeSeries;
using SlowTests.Client.TimeSeries.Query;
using SlowTests.Core.Utils.Entities;
using Sparrow;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client.TimeSeries.Issues
{
    public class RavenDB_16366 : RavenTestBase
    {
        public RavenDB_16366(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanUseDynamicGrouping()
        {
            using var store = GetDocumentStore();

            store.TimeSeries.Register<MobaroLocation, DispatchEntry>(DispatchEntryDefinition.Name);
            
            PopulateData(store);

            var grouping = GroupingInterval.Month; // This could vary based on client input

            using var session = store.OpenSession();

            var groupingAction = grouping switch
            {
                GroupingInterval.Year => (Action<ITimePeriodBuilder>)(builder => builder.Years(1)),
                GroupingInterval.Month => (Action<ITimePeriodBuilder>)(builder => builder.Months(1)),
                GroupingInterval.Day => (Action<ITimePeriodBuilder>)(builder => builder.Days(1)),
                GroupingInterval.Hour => (Action<ITimePeriodBuilder>)(builder => builder.Hours(1)),
                _ => throw new ArgumentOutOfRangeException()
            };

            var locations = session.Query<MobaroLocation>()
                .Select(location => new {
                    DispatchEntries = RavenQuery
                        .TimeSeries<DispatchEntry>(DispatchEntryDefinition.Name)
                        .GroupBy(groupingAction)
                        .Select(x => new {
                            Sum = x.Sum(),
                            Count = x.Count()
                        })
                        .ToList(),
                    LocationId = location.Id,
                    LocationName = location.Name
                })
                .ToList();

            var firstLocation = locations.First();
            var firstTimeSeriesGrouping = firstLocation.DispatchEntries.Results.First();

            Assert.Equal(expected: "locations/1", actual: firstLocation.LocationId);
            Assert.Equal(expected: 35, actual: firstTimeSeriesGrouping.Sum.Riders);
            Assert.Equal(expected: 5, actual: firstTimeSeriesGrouping.Sum.Dispatches);

            Assert.Equal(expected: 2, actual: firstTimeSeriesGrouping.Count.Riders);
            Assert.Equal(expected: 2, actual: firstTimeSeriesGrouping.Count.Dispatches);
        }

        [Fact]
        public void CanUseDynamicGrouping_WithFunc()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Jerry",
                        LastName = "Garcia"
                    }, "users/1");

                    var tsf = session.TimeSeriesFor("users/1", "HeartRate");

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
                    var grouping = GroupingInterval.Month; // This could vary based on client input

                    Action<ITimePeriodBuilder> action = builder => Func(builder, grouping);

                    var query = session.Query<User>()
                        .Select(u => RavenQuery.TimeSeries(u, "Heartrate")
                            .GroupBy(action)
                            .Select(g => new
                            {
                                Avg = g.Average(),
                                Max = g.Max()
                            })
                            .ToList());

                    var result = query.ToList();

                    Assert.Equal(1, result.Count);
                    Assert.Equal(6, result[0].Count);

                    var agg = result[0].Results;

                    Assert.Equal(2, agg.Length);

                    Assert.Equal(79, agg[0].Max[0]);
                    Assert.Equal(69, agg[0].Average[0]);

                    Assert.Equal(179, agg[1].Max[0]);
                    Assert.Equal(169, agg[1].Average[0]);
                }
            }
        }

        [Fact]
        public void CanUseDynamicGrouping_WithLambda()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Jerry",
                        LastName = "Garcia"
                    }, "users/1");

                    var tsf = session.TimeSeriesFor("users/1", "HeartRate");

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
                    var grouping = GroupingInterval.Month; // This could vary based on client input

                    var query = session.Query<User>()
                        .Select(u => RavenQuery.TimeSeries(u, "Heartrate")
                            .GroupBy(builder => Func(builder, grouping))
                            .Select(g => new
                            {
                                Avg = g.Average(),
                                Max = g.Max()
                            })
                            .ToList());

                    var result = query.ToList();

                    Assert.Equal(1, result.Count);
                    Assert.Equal(6, result[0].Count);

                    var agg = result[0].Results;

                    Assert.Equal(2, agg.Length);

                    Assert.Equal(79, agg[0].Max[0]);
                    Assert.Equal(69, agg[0].Average[0]);

                    Assert.Equal(179, agg[1].Max[0]);
                    Assert.Equal(169, agg[1].Average[0]);
                }
            }
        }

        [Fact]
        public void CanUseDynamicGrouping_WithTagAndInterpolation()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = TimeSeriesGroupByTag.PopulateCanGroupByTagWithInterpolation(store);

                using (var session = store.OpenSession())
                {
                    var grouping = GroupingInterval.Hour; // This could vary based on client input

                    var query = session.Query<TimeSeriesLinqQuery.Person>()
                        .Select(u => RavenQuery.TimeSeries(u, "Heartrate", baseline.EnsureUtc(), baseline.AddMonths(2).EnsureUtc())
                            .GroupBy(g => Func2(g, grouping, InterpolationType.Linear, true))
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
        public void CanUseDynamicGrouping_WithFuncAndLoadedTag()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = TimeSeriesGroupByTag.PopulateCanGroupByLoadedTag(store);

                using (var session = store.OpenSession())
                {
                    var interval = GroupingInterval.Month;

                    var query = session.Query<TimeSeriesLinqQuery.Person>()
                        .Select(u => RavenQuery.TimeSeries(u, "Heartrate", baseline.EnsureUtc(), baseline.AddMonths(2).EnsureUtc())
                            .GroupBy(g => Func(g, interval)
                                .ByTag<TimeSeriesLinqQuery.Watch>(w => w.Accuracy))
                            .Select(g => new
                            {
                                Average = g.Average(), 
                                Max = g.Max()
                            })
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

        private static ITimeSeriesAggregationOperations Func(ITimePeriodBuilder builder, GroupingInterval interval)
        {
            return interval == GroupingInterval.Month
                ? builder.Months(1)
                : interval == GroupingInterval.Day
                    ? builder.Days(1)
                    : interval == GroupingInterval.Hour
                        ? builder.Hours(1)
                        : builder.Years(1);
        }

        private static ITimeSeriesAggregationOperations Func2(ITimePeriodBuilder builder, GroupingInterval interval, InterpolationType interpolation, bool byTag)
        {
            var aggregationOperations = Func(builder, interval);

            if (byTag)
            {
                aggregationOperations.ByTag();
            }

            if (interpolation != InterpolationType.None)
            {
                aggregationOperations.WithOptions(new TimeSeriesAggregationOptions
                {
                    Interpolation = interpolation
                });
            }

            return aggregationOperations;
        }

        private static void PopulateData(IDocumentStore store)
        {
            using var session = store.OpenSession();

            var location = new MobaroLocation
            {
                Id = "locations/1",
                Name = "Location 1"
            };

            session.Store(location);

            var entry1 = new DispatchEntry
            {
                Riders = 15,
                Dispatches = 3
            };

            var entry2 = new DispatchEntry
            {
                Riders = 20,
                Dispatches = 2
            };

            var now = DateTime.UtcNow;
            var oneHourAgo = now.Subtract(TimeSpan.FromHours(1));

            var timeSeries = session.TimeSeriesFor<DispatchEntry>(location);
            timeSeries.Append(oneHourAgo, entry1);
            timeSeries.Append(now, entry2);

            session.SaveChanges();
        }

        private enum GroupingInterval
        {
            Year,
            Month,
            Day,
            Hour
        }

        private class DispatchEntryDefinition
        {
            public const string Name = "DispatchEntries";
        }

        private struct DispatchEntry
        {
            [TimeSeriesValue(0)]
            public double Riders;

            [TimeSeriesValue(1)]
            public double Dispatches;
        }

        private class MobaroLocation
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }
    }
}
