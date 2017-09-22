using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_8206 : RavenTestBase
    {
        [Fact]
        public void ShouldErrorAutoMapReduceIndexWhenAggregatingOnNonExistentField()
        {
            using (var store = GetDocumentStore())
            {
                store.Admin.Send(new CreateSampleDataOperation());

                using (var session = store.OpenSession())
                {
                    session.Advanced.RawQuery<dynamic>("from Products group by Category select sum(Price), Category").ToList();
                }

                var stats = store.Admin.Send(new GetIndexStatisticsOperation("Auto/Products/ByPriceReducedByCategory"));

                Assert.Equal(IndexState.Error, stats.State);
            }
        }
    }
}
