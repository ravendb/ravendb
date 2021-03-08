using System;
using FastTests;
using Raven.Client.Documents.Queries.TimeSeries;
using Xunit;
using Xunit.Abstractions;
using User = SlowTests.Core.Utils.Entities.User;

namespace SlowTests.Client.TimeSeries.Issues
{
    public class RavenDB_16221 : RavenTestBase
    {
        public RavenDB_16221(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanQueryTimeSeriesWithNumberAsName()
        {
            const string docId = "users/ayende";
            const string timeseriesName = "123";

            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User(), docId);
                    session.TimeSeriesFor(docId, timeseriesName).Append(DateTime.Now, 999);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var q = session.Advanced
                        .RawQuery<TimeSeriesRawResult>($"from Users select timeseries(from {timeseriesName})")
                        .First();

                    Assert.Equal(999, q.Results[0].Value);

                }
            }
        }
    }
}
