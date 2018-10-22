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

        public void AssertQueryResults(params (string q, int results)[] expected)
        {
            using (var store = GetDocumentStore())
            {
                store.Maintenance.Send(new CreateSampleDataOperation());

                WaitForIndexing(store);

                foreach (var item in expected)
                {
                    using (var s = store.OpenSession())
                    {
                        var results = s.Advanced.RawQuery<object>(item.q).ToList();
                        if(results.Count != item.results)
                        {
                            Assert.False(true,
                                item.q + " was suppsed to return " + item.results + " but we got " + results.Count
                                );
                        }
                    }
                }
            }
        }

        public class OrderAndProduct
        {
            public string OrderId;
            public string Product;
            public double Discount;
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

        [Fact]
        public void CanProjectEdges()
        {
            var results = Query<OrderAndProduct>(@"
match (o:Orders (id() = 'orders/821-A'))-[l:Lines.Product]->(p:Products)
select {
    OrderId: id(o),
    Product: p.Name,
    Discount: l.Discount
}
");
            Assert.Equal(3, results.Count);
            foreach (var item in results)
            {
                Assert.Equal("orders/821-A", item.OrderId);
                Assert.NotNull(item.Product);
                Assert.Equal(0.15d, item.Discount);
            }
        }

        [Fact]
        public void CanFilterIOnEdges()
        {
            // not a theory because I want to run all queries on a single db
            AssertQueryResults(
               ("match (o:Orders (id() = 'orders/828-A'))-[:Lines(ProductName = 'Chang').Product]->(p:Products)", 1),
               ("match (o:Orders (id() = 'orders/828-A'))-[:Lines(ProductName != 'Chang').Product]->(p:Products)", 2),
               ("match (o:Orders (id() = 'orders/17-A'))-[:Lines(Discount > 0).Product]->(p:Products)", 1),
               ("match (o:Orders (id() = 'orders/17-A'))-[:Lines(Discount >= 0).Product]->(p:Products)", 2),
               ("match (o:Orders (id() = 'orders/17-A'))-[:Lines(Discount <= 0.15).Product]->(p:Products)", 2),
               ("match (o:Orders (id() = 'orders/17-A'))-[:Lines(Discount < 0.15).Product]->(p:Products)", 1),
                ("match (o:Orders (id() = 'orders/828-A'))-[:Lines(ProductName in ('Spegesild', 'Chang') ).Product]->(p:Products)", 2),
                ("match (o:Orders (id() = 'orders/830-A'))-[:Lines(Discount between 0 and 0.1).Product]->(p:Products)", 24),
                ("match( e: Employees(Territories all in ('60179', '60601') ) )", 1),
                ("match(e: Employees(Territories in ('60179', '60601')) )", 1)
            );

        }
    }
}
