using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Server.Documents.Indexing.Static
{
    // Lucene reuses the field instances of dynamic fields (CreateField) in the next documents, so every value of every document must still be indexed
    public class CreateFieldInstancesReuseTests : RavenTestBase
    {
        public CreateFieldInstancesReuseTests(ITestOutputHelper output) : base(output)
        {
        }

        private class Address
        {
            public string City { get; set; }
        }

        private class OrderLine
        {
            public string Product { get; set; }
            public int Quantity { get; set; }
            public double Price { get; set; }
        }

        private class Order
        {
            public string Id { get; set; }
            public double Total { get; set; }
            public Address ShipTo { get; set; }
            public List<OrderLine> Lines { get; set; }
        }

        private const int NumberOfOrders = 64;

        private static List<Order> CreateOrders()
        {
            var random = new Random(1337);
            var orders = new List<Order>();

            for (var i = 0; i < NumberOfOrders; i++)
            {
                var lines = new List<OrderLine>();
                var numberOfLines = random.Next(1, 7);
                for (var j = 0; j < numberOfLines; j++)
                {
                    lines.Add(new OrderLine
                    {
                        Product = "products/" + random.Next(1, 10),
                        Quantity = random.Next(1, 20),
                        Price = Math.Round(random.NextDouble() * 100, 2)
                    });
                }

                orders.Add(new Order
                {
                    Id = "orders/" + i,
                    Total = lines.Sum(x => x.Quantity * x.Price),
                    ShipTo = new Address { City = "City " + (i % 5) },
                    Lines = lines
                });
            }

            return orders;
        }

        private static List<Order> Store(IDocumentStore store)
        {
            var orders = CreateOrders();

            using (var session = store.OpenSession())
            {
                foreach (var order in orders)
                    session.Store(order);

                session.SaveChanges();
            }

            return orders;
        }

        private static void PutIndex(IDocumentStore store, IndexDefinition definition)
        {
            store.Maintenance.Send(new PutIndexesOperation(definition));
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void AllValuesOfMultipleDynamicFieldsPerDocumentAreIndexed(Options options)
        {
            using var store = GetDocumentStore(options);
            var orders = Store(store);

            // several created fields per document (and a different number of them in every document), created lazily during the conversion
            PutIndex(store, new IndexDefinition
            {
                Name = "Orders/DynamicLines",
                Maps =
                {
                    @"from o in docs.Orders
select new
{
    o.Total,
    _ = o.Lines.Select(l => new [] { CreateField(""Quantity"", l.Quantity), CreateField(""Price"", l.Price), CreateField(""Product"", l.Product) })
}"
                }
            });

            Indexes.WaitForIndexing(store);
            RavenTestHelper.AssertNoIndexErrors(store);

            AssertQuantities(store, "Orders/DynamicLines", orders);
            AssertPrices(store, "Orders/DynamicLines", orders);
            AssertProducts(store, "Orders/DynamicLines", orders);
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void AllValuesOfDynamicFieldsCreatedBeforeTheConversionAreIndexed(Options options)
        {
            using var store = GetDocumentStore(options);
            var orders = Store(store);

            // one output per order line, the created field is created while the map builds the output, before its conversion
            PutIndex(store, new IndexDefinition
            {
                Name = "Orders/DynamicFanOut",
                Maps =
                {
                    @"from o in docs.Orders
from l in o.Lines
select new { l.Product, _ = CreateField(""Quantity"", l.Quantity) }"
                }
            });

            Indexes.WaitForIndexing(store);
            RavenTestHelper.AssertNoIndexErrors(store);

            AssertQuantities(store, "Orders/DynamicFanOut", orders);
        }

        [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.JavaScript)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void AllValuesOfDynamicFieldsInJavaScriptIndexAreIndexed(Options options)
        {
            using var store = GetDocumentStore(options);
            var orders = Store(store);

            PutIndex(store, new IndexDefinition
            {
                Name = "Orders/JsDynamicLines",
                Maps =
                {
                    @"map('Orders', function (o) {
    return {
        _: o.Lines.map(function (l) { return createField('Quantity', l.Quantity, { indexing: 'Exact', storage: false, termVector: null }); })
    };
})"
                }
            });

            Indexes.WaitForIndexing(store);
            RavenTestHelper.AssertNoIndexErrors(store);

            AssertQuantities(store, "Orders/JsDynamicLines", orders);
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene)]
        public void StoredComplexDynamicFieldIsProjectedAsObjectForEveryDocument(Options options)
        {
            using var store = GetDocumentStore(options);
            var orders = Store(store);

            PutIndex(store, new IndexDefinition
            {
                Name = "Orders/DynamicShipTo",
                Maps =
                {
                    @"from o in docs.Orders
select new { _ = CreateField(""ShipTo"", o.ShipTo, true, false) }"
                }
            });

            Indexes.WaitForIndexing(store);
            RavenTestHelper.AssertNoIndexErrors(store);

            using (var session = store.OpenSession())
            {
                var results = session.Advanced
                    .RawQuery<ShipToProjection>("from index 'Orders/DynamicShipTo' select ShipTo")
                    .NoTracking()
                    .ToList();

                Assert.Equal(orders.Count, results.Count);
                Assert.All(results, x => Assert.StartsWith("City ", x.ShipTo?.City));
            }
        }

        private class ShipToProjection
        {
            public Address ShipTo { get; set; }
        }

        private static void AssertQuantities(IDocumentStore store, string indexName, List<Order> orders)
        {
            var quantities = orders.SelectMany(x => x.Lines).Select(x => x.Quantity).Distinct().OrderBy(x => x).ToList();

            foreach (var quantity in quantities)
            {
                var expectedEqual = orders.Where(o => o.Lines.Any(l => l.Quantity == quantity)).Select(o => o.Id);
                AssertIds(expectedEqual, Query(store, $"from index '{indexName}' where Quantity = $v", quantity), $"{indexName}: Quantity = {quantity}");

                var expectedGreater = orders.Where(o => o.Lines.Any(l => l.Quantity > quantity)).Select(o => o.Id);
                AssertIds(expectedGreater, Query(store, $"from index '{indexName}' where Quantity > $v", quantity), $"{indexName}: Quantity > {quantity}");
            }
        }

        private static void AssertPrices(IDocumentStore store, string indexName, List<Order> orders)
        {
            for (var price = 0; price <= 100; price += 5)
            {
                var expected = orders.Where(o => o.Lines.Any(l => l.Price >= price && l.Price < price + 5)).Select(o => o.Id);
                AssertIds(expected, Query(store, $"from index '{indexName}' where Price >= {price} and Price < {price + 5}", null),
                    $"{indexName}: {price} <= Price < {price + 5}");
            }
        }

        private static void AssertProducts(IDocumentStore store, string indexName, List<Order> orders)
        {
            var products = orders.SelectMany(x => x.Lines).Select(x => x.Product).Distinct().ToList();

            foreach (var product in products)
            {
                var expected = orders.Where(o => o.Lines.Any(l => l.Product == product)).Select(o => o.Id);
                AssertIds(expected, Query(store, $"from index '{indexName}' where Product = $v", product), $"{indexName}: Product = {product}");
            }
        }

        private static List<string> Query(IDocumentStore store, string query, object value)
        {
            // a session per query, there are more queries than the requests allowed in a session
            using var session = store.OpenSession();
            var rawQuery = session.Advanced.RawQuery<Order>(query).NoTracking();
            if (value != null)
                rawQuery = rawQuery.AddParameter("v", value);

            return rawQuery.ToList().Select(x => x.Id).ToList();
        }

        private static void AssertIds(IEnumerable<string> expected, List<string> actual, string because)
        {
            var expectedSorted = expected.OrderBy(x => x, StringComparer.Ordinal).ToList();
            var actualSorted = actual.Distinct().OrderBy(x => x, StringComparer.Ordinal).ToList();

            Assert.True(expectedSorted.SequenceEqual(actualSorted),
                $"{because}: expected [{string.Join(", ", expectedSorted)}] but got [{string.Join(", ", actualSorted)}]");
        }
    }
}
