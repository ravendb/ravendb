using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Graph
{
    public class GraphQueries : RavenTestBase
    {
        public List<T> Query<T>(string q)
        {
            using (var store = GetDocumentStore())
            {
                store.Maintenance.Send(new CreateSampleDataOperation());

                WaitForIndexing(store);

                using (var s = store.OpenSession())
                {
                    return s.Advanced.RawQuery<T>(q).ToList();
                }
            }
        }

        public class OrderAndProduct
        {
            public string OrderId;
            public string Product;
        }

        [Fact]
        public void CanProjectSameDocumentTwice()
        {
            var results = Query<OrderAndProduct>(@"
match (o:Orders (id() = 'orders/828-A'))-[:Lines.Product]->(p:Products)
select {
    OrderId: id(o),
    Product: p.Name
}
");
            Assert.Equal(3, results.Count);
            foreach (var item in results)
            {
                Assert.Equal("orders/828-A", item.OrderId);
                Assert.NotNull(item.Product);
            }
        }
    }
}
