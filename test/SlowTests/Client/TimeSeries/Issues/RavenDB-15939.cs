using FastTests;
using Raven.Client.Documents.Queries.TimeSeries;
using Raven.Client.Exceptions;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client.TimeSeries.Issues
{
    public class RavenDB_15939 : RavenTestBase
    {
        public RavenDB_15939(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void ShouldThrowBetterErrorOnAttemptToGroupByTagOnly()
        {
            const string id = "employees/1";

            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    session.Store(new Employee(), id);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesAggregationResult>(@"
from Employees 
where id() == $id 
select timeseries(
 from HeartRates 
 group by tag
)")
                        .AddParameter("id", id);

                    var ex = Assert.Throws<RavenException>(() => query.First());

                    Assert.Contains(@"Expected to get time period value but got 'tag'. 
Grouping by 'Tag' or Field is supported only as a second grouping-argument", ex.Message);
                }
            }
        }

    }
}
