using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Queries.TimeSeries;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Session.TimeSeries;
using Raven.Tests.Core.Utils.Entities;
using Sparrow;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client.TimeSeries.Issues;

public class RavenDB_21649 : RavenTestBase
{
    private readonly Random _rand;
    private const string Id = "people/1";
    private const string SeriesName = "HeartRate";
    private readonly int[] _entriesPerHour = { 28, 37, 28, 38, 48, 44, 43, 33, 45, 41, 39, 41 };

    public RavenDB_21649(ITestOutputHelper output) : base(output)
    {
        _rand = new Random(12345);
    }

    [RavenFact(RavenTestCategory.TimeSeries | RavenTestCategory.Querying)]
    public void TimeSeriesAggregationWithDifferentTimeZoneOffsets_ShouldNotReturnPartialData()
    {
        using (var store = GetDocumentStore())
        {
            var baseline = new DateTime(2023, 10, 15, 14, 0, 0, DateTimeKind.Utc);

            using (var session = store.OpenSession())
            {
                session.Store(new Person(), Id);
                var tsf = session.TimeSeriesFor(Id, SeriesName);

                // insert 12 hours data
                for (var i = 0; i < _entriesPerHour.Length; i++)
                {
                    var start = baseline.AddHours(i);
                    var numOfEntries = _entriesPerHour[i];
                    AddEntriesForHour(tsf, start, numOfEntries);
                }

                session.SaveChanges();
            }

            var from = new DateTime(2023, 10, 15, 4, 0, 0, DateTimeKind.Utc);
            var to = new DateTime(2023, 10, 16, 4, 0, 0, DateTimeKind.Utc);

            using (var session = store.OpenSession())
            {
                var aggregationWithNoOffset = session.Query<Person>()
                    .Where(p => p.Id == Id)
                    .Select(p => RavenQuery.TimeSeries(p, SeriesName, from, to)
                        .GroupBy(x => x.Hours(1))
                        .Select(x => x.Sum())
                        .ToList())
                    .SingleOrDefault()?.Results;

                Assert.NotNull(aggregationWithNoOffset);
                Assert.Equal(12, aggregationWithNoOffset.Length);
                Assert.Equal(baseline, aggregationWithNoOffset[0].From);
                Assert.Equal(baseline.AddHours(12), aggregationWithNoOffset[^1].To);

                // check all timezone offsets 
                for (var i = -12; i <= 14; i++)
                {
                    if (i == 0) 
                        continue;

                    var offset = TimeSpan.FromHours(i);

                    var aggregationWithOffset = session.Query<Person>()
                        .Where(p => p.Id == Id)
                        .Select(p => RavenQuery.TimeSeries(p, SeriesName, from, to)
                            .GroupBy(x => x.Hours(1))
                            .Select(x => x.Sum())
                            .Offset(offset)
                            .ToList())
                        .SingleOrDefault()?.Results;

                    AssertAggregationResults(aggregationWithNoOffset, aggregationWithOffset, offset);
                }
            }
        }
    }

    [RavenFact(RavenTestCategory.TimeSeries | RavenTestCategory.Querying)]
    public void TimeSeriesQueryWithDifferentTimeZoneOffsets_ShouldNotReturnPartialData()
    {
        using (var store = GetDocumentStore())
        {
            var baseline = new DateTime(2023, 10, 15, 14, 0, 0, DateTimeKind.Utc);

            using (var session = store.OpenSession())
            {
                session.Store(new Person(), Id);
                var tsf = session.TimeSeriesFor(Id, SeriesName);

                // insert 12 hours data
                for (var i = 0; i < _entriesPerHour.Length; i++)
                {
                    var start = baseline.AddHours(i);
                    var numOfEntries = _entriesPerHour[i];
                    AddEntriesForHour(tsf, start, numOfEntries);
                }

                session.SaveChanges();
            }

            var from = new DateTime(2023, 10, 15, 4, 0, 0, DateTimeKind.Utc);
            var to = new DateTime(2023, 10, 16, 4, 0, 0, DateTimeKind.Utc);

            using (var session = store.OpenSession())
            {
                var queryWithNoOffset = session.Query<Person>()
                    .Where(p => p.Id == Id)
                    .Select(p => RavenQuery.TimeSeries(p, SeriesName, from, to).ToList())
                    .SingleOrDefault()?.Results;

                Assert.NotNull(queryWithNoOffset);
                Assert.Equal(_entriesPerHour.Sum(), queryWithNoOffset.Length);
                Assert.True(baseline <= queryWithNoOffset[0].Timestamp);
                Assert.True(baseline.AddHours(12) >= queryWithNoOffset[^1].Timestamp);

                // check all timezone offsets 
                for (var i = -12; i <= 14; i++)
                {
                    if (i == 0)
                        continue;

                    var offset = TimeSpan.FromHours(i);

                    var queryWithOffset = session.Query<Person>()
                        .Where(p => p.Id == Id)
                        .Select(p => RavenQuery.TimeSeries(p, SeriesName, from, to)
                            .Offset(offset)
                            .ToList())
                        .SingleOrDefault()?.Results;

                    AssertRawResults(queryWithNoOffset, queryWithOffset, offset);
                }
            }

        }
    }

    [RavenFact(RavenTestCategory.TimeSeries | RavenTestCategory.Querying)]
    public void TimeSeriesStreamQueryWithOffset_ShouldNotReturnPartialData()
    {
        using (var store = GetDocumentStore())
        {
            var numOfEntries = TimeSpan.FromHours(1).TotalMinutes;
            var baseline = DateTime.UtcNow.EnsureMilliseconds();

            using (var session = store.OpenSession())
            {
                session.Store(new User(), "karmel");
                var ts = session.TimeSeriesFor("karmel", "heartrate");
                for (int i = 0; i < numOfEntries; i++)
                {
                    ts.Append(baseline.AddMinutes(i), i);
                }
                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                var ts = session.TimeSeriesFor("karmel", "heartrate");
                var from = baseline;
                var to = baseline.AddHours(1);

                var offset = TimeSpan.FromHours(12);
                var baselineWithOffset = baseline.Add(offset);

                using (var it = ts.Stream(from, to, offset: offset))
                {
                    var i = 0;
                    while (it.MoveNext())
                    {
                        var entry = it.Current;
                        Assert.Equal(baselineWithOffset.AddMinutes(i), entry.Timestamp);
                        Assert.Equal(i, entry.Value);
                        i++;
                    }

                    Assert.Equal(numOfEntries, i);
                }
            }
        }
    }

    private static void AssertAggregationResults(IReadOnlyList<TimeSeriesRangeAggregation> originalResults, IReadOnlyList<TimeSeriesRangeAggregation> aggregationResult, TimeSpan offset)
    {
        Assert.NotNull(aggregationResult);
        Assert.Equal(originalResults.Count, aggregationResult.Count);

        for (int i = 0; i < originalResults.Count; i++)
        {
            var original = originalResults[i];
            var withOffset = aggregationResult[i];

            // from/to dates should be the same when offset added to original results 
            Assert.Equal(original.From.Add(offset), withOffset.From);
            Assert.Equal(original.To.Add(offset), withOffset.To);

            // count and sum should be identical
            Assert.True(original.Count.SequenceEqual(withOffset.Count));
            Assert.True(original.Sum.SequenceEqual(withOffset.Sum));
        }
    }

    private static void AssertRawResults(TimeSeriesEntry[] originalResults, TimeSeriesEntry[] aggregationResult, TimeSpan offset)
    {
        Assert.NotNull(aggregationResult);
        Assert.Equal(originalResults.Length, aggregationResult.Length);

        for (int i = 0; i < originalResults.Length; i++)
        {
            var original = originalResults[i];
            var withOffset = aggregationResult[i];

            // timestamp should be the same when offset added to original results 
            Assert.Equal(original.Timestamp.Add(offset), withOffset.Timestamp);

            // values should be identical
            Assert.True(original.Values.SequenceEqual(withOffset.Values));
        }
    }

    private void AddEntriesForHour(ISessionDocumentAppendTimeSeriesBase tsf, DateTime baseline, int numOfEntries)
    {
        var chosen = new HashSet<int>();

        for (int i = 0; i < numOfEntries; i++)
        {
            int minutesToAdd;
            while (true)
            {
                minutesToAdd = _rand.Next(0, 60);
                if (chosen.Add(minutesToAdd))
                    break;
            }

            var timestamp = baseline.AddMinutes(minutesToAdd);
            tsf.Append(timestamp, new[] { minutesToAdd, minutesToAdd * 1.5, minutesToAdd * 0.75, 0, minutesToAdd * 1.15 });
        }
    }
}
