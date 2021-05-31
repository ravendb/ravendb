using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Queries.TimeSeries;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client.TimeSeries.Issues
{
    public class RavenDB_15452 : RavenTestBase
    {
        public RavenDB_15452(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanGroupByMilliseconds()
        {
            using (var store = GetDocumentStore())
            {
                var documentId = "users/ayende";
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    session.Store(new User(), documentId);

                    var tsf = session.TimeSeriesFor(documentId, "HeartRate");
                    for (int i = 0; i < TimeSpan.FromSeconds(1).TotalMilliseconds; i++)
                    {
                        tsf.Append(baseline.AddMilliseconds(i), i);
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var result = session.Query<User>()
                        .Where(u => u.Id == documentId)
                        .Select(u => RavenQuery.TimeSeries(u, "HeartRate", baseline, baseline.AddSeconds(1))
                            .GroupBy(g => g.Milliseconds(500))
                            .Select(x => x.Max())
                            .ToList())
                        .First();

                    Assert.Equal(TimeSpan.FromSeconds(1).TotalMilliseconds, result.Count);
                    Assert.Equal(2, result.Results.Length);

                    var rangeAggregation = result.Results[0];

                    Assert.Equal(500, rangeAggregation.Count[0]);
                    Assert.Equal(499, rangeAggregation.Max[0]);
                    Assert.Equal(baseline, rangeAggregation.From);
                    Assert.Equal(baseline.AddMilliseconds(500), rangeAggregation.To);

                    rangeAggregation = result.Results[1];

                    Assert.Equal(500, rangeAggregation.Count[0]);
                    Assert.Equal(999, rangeAggregation.Max[0]);
                    Assert.Equal(baseline.AddMilliseconds(500), rangeAggregation.From);
                    Assert.Equal(baseline.AddSeconds(1), rangeAggregation.To);
                }

                using (var session = store.OpenSession())
                {
                    var result = session.Advanced.RawQuery<TimeSeriesAggregationResult>(@"
from Users 
where id() = $id
select timeseries(
    from HeartRate 
    between $start and $end
    group by 100 milliseconds
    select max()
)")
                        .AddParameter("id", documentId)
                        .AddParameter("start", baseline)
                        .AddParameter("end", baseline.AddMinutes(1))
                        .First();

                    Assert.Equal(TimeSpan.FromSeconds(1).TotalMilliseconds, result.Count);
                    Assert.Equal(10, result.Results.Length);

                    foreach (var rangeAggregation in result.Results)
                    {
                        Assert.Equal(100, rangeAggregation.Count[0]);
                    }
                }
            }
        }
    }
}
