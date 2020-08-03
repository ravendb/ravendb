using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Queries.TimeSeries;
using Raven.Client.Documents.Session;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client.TimeSeries.Issues
{
    public class RavenDB_13592 : RavenTestBase
    {
        public RavenDB_13592(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void TestFillingMissingGaps()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    session.Store(new User(), "users/ayende");

                    var tsf = session.TimeSeriesFor("users/ayende", "Heartrate");

                    tsf.Append(baseline, 60);
                    tsf.Append(baseline.AddMinutes(10), 61);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())    
                {
                    /*var query = session.Query<User>()
                        .Where(u => u.Id == "users/ayende")
                        .Select(u =>
                        RavenQuery.TimeSeries(u, "Heartrate", baseline, baseline.AddHours(1))
                            .GroupBy(g => g.Minutes(1))
                            .Select(x => x.Max())
                            .ToList());*/

                    var q = session.Advanced.RawQuery<TimeSeriesAggregationResult>(@"
from Users as u
where id(u) == $id
select timeseries(
    from u.Heartrate
    between $start and $end
    group by 1 minute
    select max()
)")
                        .AddParameter("id", "users/ayende")
                        .AddParameter("start", baseline)
                        .AddParameter("end", baseline.AddHours(1));

                    var result = q.First();
                    Assert.Equal(2, result.Count);
                    Assert.Equal(10, result.Results.Length);

                }
            }
        }
    }
}
