using System;
using FastTests;
using Orders;
using Raven.Client.Documents.Session.TimeSeries;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_15129 : RavenTestBase
    {
        public RavenDB_15129(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void TimeSeriesValue_RequiresDoubleType()
        {
            using (var store = GetDocumentStore())
            {
                var e = Assert.Throws<InvalidOperationException>(() => store.TimeSeries.Register<Company, MetricValue>());
                Assert.Contains("Cannot create a mapping for", e.Message);
            }
        }

        private class MetricValue
        {
            [TimeSeriesValue(0)] public long DurationInMs;
            [TimeSeriesValue(1)] public long RequestSize;
            [TimeSeriesValue(2)] public string SourceIp;
        }
    }
}
