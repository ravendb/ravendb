using System;
using System.Linq;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Queries.TimeSeries;
using Raven.Tests.Core.Utils.Entities;
using Sparrow;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client.TimeSeries.Issues
{
    public class RavenDB_15732 : ReplicationTestBase
    {
        public RavenDB_15732(ITestOutputHelper output) : base(output)
        {
        }


        [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.TimeSeries | RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void RavenDB_15732_1(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var baseline = new DateTime(2019, 12, 17).EnsureUtc();
                var id = "companies/1";
                var total = TimeSpan.FromDays(20).TotalHours;

                using (var session = store.OpenSession())
                {
                    session.Store(new Company
                    {
                        Name = "HR",
                    }, id);

                    var tsf = session.TimeSeriesFor(id, "StockPrices");

                    for (int i = 0; i <= total; i++)
                    {
                        tsf.Append(baseline.AddHours(i), i);
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var result = session.Advanced.RawQuery<TimeSeriesAggregationResult>(@"
from Companies
where id() == $id
select timeseries(
    from StockPrices 
    between '2019-12-15' and '2020-01-01' 
    group by 7d select avg()
)
")
                        .AddParameter("id", id)
                        .First();

                    var expected = new DateTime(2019, 12, 15).EnsureUtc();
                    Assert.Equal(expected, result.Results[0].From);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.TimeSeries | RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void RavenDB_15732_2(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var baseline = new DateTime(2019, 12, 24).EnsureUtc();
                var id = "companies/1";
                var total = TimeSpan.FromDays(20).TotalHours;

                using (var session = store.OpenSession())
                {
                    session.Store(new Company
                    {
                        Name = "HR",
                    }, id);

                    var tsf = session.TimeSeriesFor(id, "StockPrices");

                    for (int i = 0; i <= total; i++)
                    {
                        tsf.Append(baseline.AddHours(i), i);
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var result = session.Advanced.RawQuery<TimeSeriesAggregationResult>(@"
from Companies
where id() == $id
select timeseries(
    from StockPrices 
    between '2019-12-15' and '2020-01-01' 
    group by 7d select avg()
)
")
                        .AddParameter("id", id)
                        .First();

                    var expected = new DateTime(2019, 12, 22).EnsureUtc(); // one week after '2019-12-15'
                    Assert.Equal(expected, result.Results[0].From);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.TimeSeries | RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void RavenDB15732_3(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var baseline = new DateTime(2019, 12, 24).EnsureUtc();
                var id = "companies/1";
                var total = TimeSpan.FromDays(15).TotalHours;

                using (var session = store.OpenSession())
                {
                    session.Store(new Company
                    {
                        Name = "HR",
                    }, id);

                    var tsf = session.TimeSeriesFor(id, "StockPrices");

                    for (int i = 0; i <= total; i++)
                    {
                        tsf.Append(baseline.AddHours(i), i);
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var q = session.Query<Company>()
                        .Where(c => c.Id == id)
                        .Select(c => RavenQuery.TimeSeries(c, "StockPrices")
                            .GroupBy(g => g.Days(7))
                            .Select(x => x.Average())
                            .ToList());

                    var result = q.First();

                    var expected = new DateTime(2019, 12, 23).EnsureUtc();

                    Assert.Equal(expected, result.Results[0].From);
                    Assert.Equal(DayOfWeek.Monday, result.Results[0].From.DayOfWeek);
                }
            }
        }

    }
}
