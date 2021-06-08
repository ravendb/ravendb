using System;
using FastTests;
using Raven.Client.Documents.Queries.TimeSeries;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client.TimeSeries.Issues
{
    public class RavenDB_15029 : RavenTestBase
    {
        public RavenDB_15029(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void SessionRawQueryShouldNotTrackTimeSeriesResultAsDocument()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;
                using (var session = store.OpenSession())
                {
                    session.Store(new User {Name = "Karmel"}, "users/karmel");
                    session.TimeSeriesFor("users/karmel", "HeartRate").Append(baseline, 60, "watches/fitbit");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var u = session.Load<User>("users/karmel");
                    var query = session.Advanced.RawQuery<TimeSeriesRawResult>(@"
declare timeseries out() 
{
    from HeartRate 
}
from Users as u
where Name = 'Karmel'
select out()
");
                    var result = query.First();

                    Assert.Equal(1, result.Count);
                    Assert.Equal(60, result.Results[0].Value);
                    Assert.Equal(baseline, result.Results[0].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal("watches/fitbit", result.Results[0].Tag);
                }
            }
        }
    }
}
