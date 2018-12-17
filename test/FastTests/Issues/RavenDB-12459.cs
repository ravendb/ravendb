using System.Linq;
using Newtonsoft.Json.Linq;
using Raven.Client.Exceptions;
using Xunit;

namespace FastTests.Issues
{
    public class RavenDB_12459 : RavenTestBase
    {
        [Fact]
        public void Should_fail_on_missing_projection_for_edge_arrays()
        {
            using (var store = GetDocumentStore())
            {
                CreateNorthwindDatabase(store);

                using (var session = store.OpenSession())
                {
                  
                    //the issue was that this query throws NRE at server-side.
                    //instead this should throw proper InvalidQueryException
                    Assert.Throws<InvalidQueryException>(() => session.Advanced.RawQuery<JObject>(
                        @"match (Orders as o where Company = 'companies/72-A')
                            -[Lines where Discount > 0]->(Products as p)"
                    ).ToList());
                }
            }
        }

        [Fact]
        public void Should_fail_on_missing_projection_for_single_edge()
        {
            using (var store = GetDocumentStore())
            {
                CreateNorthwindDatabase(store);
                using (var session = store.OpenSession())
                {
                    //the issue was that this query throws NRE at server-side.
                    //instead this should throw proper InvalidQueryException
                    Assert.Throws<InvalidQueryException>(() => session.Advanced.RawQuery<JObject>(
                        "match (Employees as e)-[Address where City = 'London']->(_ as l)").ToList());
                }
            }
        }

        [Fact]
        public void Should_fail_on_invalid_projection_for_edge_arrays()
        {
            using (var store = GetDocumentStore())
            {
                CreateNorthwindDatabase(store);
                using (var session = store.OpenSession())
                {
                    //the issue was that this query throws NRE at server-side.
                    //instead this should throw proper InvalidQueryException
                    Assert.Throws<InvalidQueryException>(() => session.Advanced.RawQuery<JObject>(
                        @"match (Orders as o where Company = 'companies/72-A')
                            -[Lines where Discount > 0 select product,discount]->(Products as p)"
                    ).ToList());
                }
            }
        }
    }
}
