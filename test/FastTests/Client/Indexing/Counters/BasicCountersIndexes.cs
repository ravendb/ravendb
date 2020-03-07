using Raven.Client.Documents.Indexes.Counters;
using Raven.Client.Documents.Operations.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Client.Indexing.Counters
{
    public class BasicCountersIndexes : RavenTestBase
    {
        public BasicCountersIndexes(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void BasicMapIndex()
        {
            using (var store = GetDocumentStore())
            {
                var result = store.Maintenance.Send(new PutIndexesOperation(new CountersIndexDefinition
                {
                    Name = "MyCountersIndex",
                    Maps = {
                    "from counter in counters.Companies.HeartRate " +
                    "from value in counter.Values " +
                    "select new { " +
                    "   Value = value, " +
                    "   Name = counter.Name," +
                    "   User = counter.DocumentId " +
                    "}" }
                }));
            }
        }
    }
}
