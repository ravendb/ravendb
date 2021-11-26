using System.Linq;
using FastTests;
using FastTests.Client;
using Orders;
using Raven.Client.Documents;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_17574 : RavenTestBase
    {
        public RavenDB_17574(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void document_query_with_projection_and_orderby_score_afterwards()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var query =
                        session.Advanced.DocumentQuery<Order>()
                            .ToQueryable()
                            .Select(x => new
                            {
                                x.Freight,
                                x.Company
                            })
                            .OrderByScoreDescending();

                    Assert.Equal("from 'Orders' order by score() desc select Freight, Company", query.ToString());
                }
            }
        }
    }
}
