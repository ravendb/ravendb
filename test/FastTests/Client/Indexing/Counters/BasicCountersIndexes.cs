using System.Threading;
using Raven.Client.Documents.Indexes.Counters;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Tests.Core.Utils.Entities;
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
                    "from counter in counters.Companies.Likes " +
                    "select new { " +
                    "   Value = counter.Value, " +
                    "   Name = counter.Name," +
                    "   User = counter.DocumentId " +
                    "}" }
                }));

                using (var session = store.OpenSession())
                {
                    var company = new Company { Name = "HR" };
                    session.Store(company);
                    session.CountersFor(company).Increment("Likes", 11);

                    session.SaveChanges();
                }

                Thread.Sleep(100000);
            }
        }
    }
}
