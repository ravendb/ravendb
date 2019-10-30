using Raven.Client.Documents.Indexes.TimeSeries;
using Raven.Client.Documents.Operations.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Client.Indexing.TimeSeries
{
    public class BasicTimeSeriesIndexes : RavenTestBase
    {
        public BasicTimeSeriesIndexes(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void T1()
        {
            using (var store = GetDocumentStore())
            {
                var result = store.Maintenance.Send(new PutIndexesOperation(new TimeSeriesIndexDefinition
                {
                    Name = "MyTsIndex",
                    Maps = { "from ts in timeSeries.People.HeartRate from entry in ts.Entries select new { HeartBeat = entry.Values[0], Date = entry.TimeStamp.Date, User = ts.Id }" }
                }));
            }
        }
    }
}
