using System.Linq;
using FastTests;
using FastTests.Server.Basic.Entities;
using Newtonsoft.Json.Linq;
using Raven.Client.Documents.Indexes;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_12542 : RavenTestBase
    {
        [Fact]
        public void Single_node_index_query_should_work()
        {
            using (var store = GetDocumentStore())
            {
                CreateNorthwindDatabase(store);
                WaitForIndexing(store);
                using (var session = store.OpenSession())
                {
                    var queryResultsFromIndex =
                        session.Advanced.RawQuery<JObject>("match (from index 'Orders/Totals')").ToArray();

                    var queryResultsFromCollection =
                        session.Advanced.RawQuery<JObject>("match (Orders as o)").ToArray();

                    Assert.Equal(queryResultsFromCollection, queryResultsFromIndex);

                }
            }
        }

        [Fact]
        public void Pattern_match_node_index_query_should_work()
        {
            using (var store = GetDocumentStore())
            {
                CreateNorthwindDatabase(store);
                new IndexOrdersProductsWithPricePerUnit().Execute(store);
                WaitForIndexing(store);
                using (var session = store.OpenSession())
                {
                    var queryResults =
                        session.Advanced.RawQuery<JObject>(
                            @"match (from index 'Orders/Totals')-[Lines where PricePerUnit > 200 select Product]->(from index 'Product/Search' as ps)
                              select id(ps) as ProductId
                             ").ToArray().Select(x => x["ProductId"].Value<string>()).ToArray();

                    var referenceQueryResults = session.Advanced.RawQuery<Order>(@"from index 'Orders/ProductsWithPricePerUnit' where PricePerUnit > 200")
                        .ToArray()
                        .SelectMany(x => x.Lines).ToArray().Where(x => x.PricePerUnit > 200).Select(x =>x.Product).ToArray();

                    Assert.Equal(referenceQueryResults, queryResults);
                }
            }
        }

        public class IndexOrdersProductsWithPricePerUnit : AbstractIndexCreationTask
        {
            public override string IndexName => "Orders/ProductsWithPricePerUnit";

            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    Maps =
                    {
                          @"from order in docs.Orders
                            from orderLine in order.Lines
                            select new { Product = orderLine.Product, PricePerUnit = orderLine.PricePerUnit }"
                    }
                };
            }
        }
    }
}
