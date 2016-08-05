using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client;
using Raven.Client.Indexes;
using Raven.Client.Linq;
using Xunit;

namespace SlowTests.Tests.ResultsTransformer
{
    public class StronglyTypedResultsTransformer : RavenTestBase
    {
        private class Product
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }
        private class Order
        {
            public string Id { get; set; }
            public string CustomerId { get; set; }
            public string[] ProductIds { get; set; }
        }

        private class Customer
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public string ZipCode { get; set; }
        }

        private class OrderWithProductInformationMultipleReturns : AbstractTransformerCreationTask<Order>
        {
            public class Result
            {
                public string OrderId { get; set; }
                public string ProductId { get; set; }
                public string ProductName { get; set; }
            }
            public OrderWithProductInformationMultipleReturns()
            {
                TransformResults = orders => from doc in orders
                                             from productid in doc.ProductIds
                                             let product = LoadDocument<Product>(productid)
                                             select new
                                             {
                                                 OrderId = doc.Id,
                                                 ProductId = product.Id,
                                                 ProductName = product.Name
                                             };
            }
        }
        private class OrderWithProductInformation : AbstractTransformerCreationTask<Order>
        {
            public class Result
            {
                public string OrderId { get; set; }
                public string CustomerId { get; set; }
                public ResultProduct[] Products { get; set; }
            }

            public class ResultProduct
            {
                public string ProductId { get; set; }
                public string ProductName { get; set; }
            }
            public OrderWithProductInformation()
            {
                TransformResults = orders => from doc in orders
                                             select new
                                             {
                                                 OrderId = doc.Id,
                                                 Products = from productid in doc.ProductIds
                                                            let product = LoadDocument<Product>(productid)
                                                            select new
                                                            {
                                                                ProductId = product.Id,
                                                                ProductName = product.Name
                                                            }
                                             };
            }
        }

        [Fact]
        public async Task CanUseResultsTransformerOnLoadWithRemoteDatabase()
        {
            using (var store = await GetDocumentStore())
            {
                PerformLoadingTest(store);
            }
        }

        [Fact]
        public async Task CanUseResultsTransformerOnLoadWithLocalDatabase()
        {
            using (var store = await GetDocumentStore())
            {
                PerformLoadingTest(store);
            }
        }

        [Fact]
        public async Task CanUseResultsTransformerOnLoadWithMultipleReturnsWithRemoteDatabase()
        {
            using (var store = await GetDocumentStore())
            {
                PerformLoadingTestArray(store);
            }
        }

        [Fact]
        public async Task CanUseResultsTransformerOnLoadWithMultipleReturnsWithLocalDatabase()
        {
            using (var store = await GetDocumentStore())
            {
                PerformLoadingTestArray(store);
            }
        }

        [Fact]
        public async Task CannotUseResultsTransformerOnLoadWithMultipleReturnsWithRemoteDatabaseWithSingleExpectation()
        {
            using (var store = await GetDocumentStore())
            {
                Assert.Throws<InvalidOperationException>(() =>
                {
                    PerformLoadingTestArrayWithSingleExpectation(store);
                });
            }
        }

        [Fact]
        public async Task CanUseResultsTransformerOnLoadWithMultipleReturnsWithLocalDatabaseWithSingleExpectation()
        {
            using (var store = await GetDocumentStore())
            {
                Assert.Throws<InvalidOperationException>(() =>
                {
                    PerformLoadingTestArrayWithSingleExpectation(store);
                });
            }
        }


        [Fact]
        public async Task CanUseResultsTransformerOnDynamicQueryWithRemoteDatabase()
        {
            using (var store = await GetDocumentStore())
            {
                PerformBasicTestWithDynamicQuery(store);
            }
        }

        [Fact]
        public async Task CanUseResultsTransformerOnDynamicQueryWithInMemoryDatabase()
        {
            using (var store = await GetDocumentStore())
            {
                PerformBasicTestWithDynamicQuery(store);
            }
        }

        private void PerformLoadingTestArrayWithSingleExpectation(IDocumentStore store)
        {
            new OrderWithProductInformationMultipleReturns().Execute(store);

            using (var session = store.OpenSession())
            {
                session.Store(new Product { Name = "Milk", Id = "products/milk" });
                session.Store(new Product { Name = "Bear", Id = "products/bear" });

                session.Store(new Order
                {
                    Id = "orders/1",
                    CustomerId = "customers/ayende",
                    ProductIds = new[] { "products/milk", "products/bear" }
                });
                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                var products = session.Load<OrderWithProductInformationMultipleReturns, OrderWithProductInformationMultipleReturns.Result>("orders/1");
            }
        }

        private void PerformLoadingTestArray(IDocumentStore store)
        {
            new OrderWithProductInformationMultipleReturns().Execute(store);

            using (var session = store.OpenSession())
            {
                session.Store(new Product { Name = "Milk", Id = "products/milk" });
                session.Store(new Product { Name = "Bear", Id = "products/bear" });

                session.Store(new Order
                {
                    Id = "orders/1",
                    CustomerId = "customers/ayende",
                    ProductIds = new[] { "products/milk", "products/bear" }
                });
                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                var products = session.Load<OrderWithProductInformationMultipleReturns, OrderWithProductInformationMultipleReturns.Result[]>("orders/1");
                products = products.OrderBy(x => x.ProductId).ToArray();

                Assert.Equal("products/bear", products[0].ProductId);
                Assert.Equal("products/milk", products[1].ProductId);
            }
        }

        private void PerformLoadingTest(IDocumentStore store)
        {

            new OrderWithProductInformation().Execute(store);

            using (var session = store.OpenSession())
            {
                session.Store(new Product { Name = "Milk", Id = "products/milk" });
                session.Store(new Product { Name = "Bear", Id = "products/bear" });

                session.Store(new Order
                {
                    Id = "orders/1",
                    CustomerId = "customers/ayende",
                    ProductIds = new[] { "products/milk" }
                });
                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                var order = session.Load<OrderWithProductInformation, OrderWithProductInformation.Result>("orders/1");

                order.Products = order.Products.OrderBy(x => x.ProductName).ToArray();

                Assert.Equal("Milk", order.Products[0].ProductName);
                Assert.Equal("products/milk", order.Products[0].ProductId);
            }
        }

        private void PerformBasicTestWithDynamicQuery(IDocumentStore store)
        {
            new OrderWithProductInformation().Execute(store);

            using (var session = store.OpenSession())
            {
                session.Store(new Product { Name = "Milk", Id = "products/milk" });
                session.Store(new Product { Name = "Bear", Id = "products/bear" });

                session.Store(new Order { CustomerId = "customers/ayende", ProductIds = new[] { "products/milk" } });
                session.Store(new Order { CustomerId = "customers/ayende", ProductIds = new[] { "products/milk" } });
                session.Store(new Order { CustomerId = "customers/ayende", ProductIds = new[] { "products/bear", "products/milk" } });

                session.Store(new Order { CustomerId = "customers/rahien", ProductIds = new[] { "products/bear" } });
                session.Store(new Order { CustomerId = "customers/bob", ProductIds = new[] { "products/bear", "products/milk" } });

                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                var customer = session.Query<Order>()
                    .Customize(x => x.WaitForNonStaleResults())
                    .Where(order => order.CustomerId == "customers/bob")
                    .TransformWith<OrderWithProductInformation, OrderWithProductInformation.Result>()
                    .Single();

                customer.Products = customer.Products.OrderBy(x => x.ProductName).ToArray();

                Assert.Equal("Milk", customer.Products[1].ProductName);
                Assert.Equal("products/milk", customer.Products[1].ProductId);

                Assert.Equal("Bear", customer.Products[0].ProductName);
                Assert.Equal("products/bear", customer.Products[0].ProductId);
            }
        }
    }

}
