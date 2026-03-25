using System;
using FastTests;
using Raven.Client.Documents.Queries.TimeSeries;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_25542(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenTheory(RavenTestCategory.TimeSeries | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void CanParseTimeSeriesKeywordInAnyCase(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            var id = "units/1";
            var baseline = RavenTestHelper.UtcToday;
            var end = baseline.AddHours(1);

            using (var session = store.OpenSession())
            {
                session.Store(new Unit { Id = id });

                var powerTs = session.TimeSeriesFor(id, "Power");
                var waterTs = session.TimeSeriesFor(id, "Water");
                powerTs.Append(baseline.AddMinutes(1), 100);
                waterTs.Append(baseline.AddMinutes(2), 200);
                session.SaveChanges();
                WaitForUserToContinueTheTest(store);
            }

            using (var session = store.OpenSession())
            {
                var baseTestQuery = @"
from Units
where id() = $id
select
timeSeries(
    from 'Power' between $start and $end group by '1 hours'
    select sum()
) as Power,
timeSeries(
    from 'Water' between $start and $end group by '1 hours'
    select sum()
) as Water
";
          
                
                var query = session.Advanced.RawQuery<TimeSeriesResult>(baseTestQuery.Replace("timeSeries", "timeseries"))
                    .WaitForNonStaleResults()
                    .AddParameter("id", id)
                    .AddParameter("start", baseline)
                    .AddParameter("end", end);

                AssertResult(query.Single());

                query = session.Advanced.RawQuery<TimeSeriesResult>(baseTestQuery)
                    .AddParameter("id", id)
                    .AddParameter("start", baseline)
                    .AddParameter("end", end);
                
                AssertResult(query.Single());
            }
        }
    }

    private static void AssertResult(TimeSeriesResult result)
    {
        Assert.NotNull(result.Power);
        Assert.NotNull(result.Water);

        Assert.Equal(1, result.Power.Results.Length);
        Assert.Equal(100, result.Power.Results[0].Sum[0]);

        Assert.Equal(1, result.Water.Results.Length);
        Assert.Equal(200, result.Water.Results[0].Sum[0]);
    }

    private class Unit
    {
        public string Id { get; set; }
    }

    private class TimeSeriesResult
    {
        public TimeSeriesAggregationResult Power { get; set; }
        public TimeSeriesAggregationResult Water { get; set; }
    }
}
