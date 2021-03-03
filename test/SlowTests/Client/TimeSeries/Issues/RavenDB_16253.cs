using System;
using FastTests;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client.TimeSeries.Issues
{
    public class RavenDB_16253 : RavenTestBase
    {
        public RavenDB_16253(ITestOutputHelper output) : base(output)
        {
        }

        private const string DocId = "users/ayende";

        [Fact]
        public void CanAppendMinValueTimestamp()
        {
            using (var store = GetDocumentStore())
            {
                var timeSeries = "HeartRate";

                using (var session = store.OpenSession())
                {
                    session.Store(new User(), DocId);
                    var timeSeriesFor = session.TimeSeriesFor(DocId, timeSeries);
                    timeSeriesFor.Append(DateTime.MinValue, 0, "watches/fitbit");
                    session.SaveChanges();
                }
            }
        }
    }
}
