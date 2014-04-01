using System;
using System.Linq;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;
using Raven.Client;
using Raven.Client.Linq;

namespace Raven.Tests.ResultsTransformer
{
    public class StronglyTypedResultsTransformer : RavenTest
    {
        public class Product
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }
        public class Order
        {
            public string Id { get; set; }
            public string CustomerId { get; set; }
            public string[] ProductIds { get; set; }
        }
        public class Customer
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public string ZipCode { get; set; }
        }

        public class OrderWithProductInformationMultipleReturns : AbstractTransformerCreationTask<Order>
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
        public class OrderWithProductInformation : AbstractTransformerCreationTask<Order>
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
        public void CanUseResultsTransformerOnLoadWithRemoteDatabase()
        {
            using (var store = NewRemoteDocumentStore())
            {
                PerformLoadingTest(store);
            }
        }

        [Fact]
        public void CanUseResultsTransformerOnLoadWithLocalDatabase()
        {
            using (var store = NewDocumentStore())
            {
                PerformLoadingTest(store);
            }
        }

        [Fact]
        public void CanUseResultsTransformerOnLoadWithMultipleReturnsWithRemoteDatabase()
        {
            using (var store = NewRemoteDocumentStore())
            {
                PerformLoadingTestArray(store);
            }
        }

        [Fact]
        public void CanUseResultsTransformerOnLoadWithMultipleReturnsWithLocalDatabase()
        {
            using (var store = NewDocumentStore())
            {
                PerformLoadingTestArray(store);
            }
        }

        [Fact]
        public void CannotUseResultsTransformerOnLoadWithMultipleReturnsWithRemoteDatabaseWithSingleExpectation()
        {
            using (var store = NewRemoteDocumentStore())
            {
                Assert.Throws<InvalidOperationException>(() =>
                {
                    PerformLoadingTestArrayWithSingleExpectation(store);
                });
            }
        }

        [Fact]
        public void CanUseResultsTransformerOnLoadWithMultipleReturnsWithLocalDatabaseWithSingleExpectation()
        {
            using (var store = NewDocumentStore())
            {
                Assert.Throws<InvalidOperationException>(() =>
                {
                    PerformLoadingTestArrayWithSingleExpectation(store);
                });
            }
        }


        [Fact]
        public void CanUseResultsTransformerOnDynamicQueryWithRemoteDatabase()
        {
            using (var store = NewRemoteDocumentStore())
            {
                PerformBasicTestWithDynamicQuery(store);
            }
        }

        [Fact]
        public void CanUseResultsTransformerOnDynamicQueryWithInMemoryDatabase()
        {
            using (var store = NewDocumentStore())
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
