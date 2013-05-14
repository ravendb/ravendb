// -----------------------------------------------------------------------
//  <copyright file="IndexedProperties.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Linq;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Client.Indexes;
using Raven.Json.Linq;
using Raven.Abstractions.Extensions;
using Xunit;
using System;

namespace Raven.Tests.Bundles.IndexedProperties
{
	public class IndexedProperties : RavenTest
	{
		private class Customer
		{
			public string Id { get; set; }
			public string Name { get; set; }
            public decimal TotalAmount { get; set; }
			public decimal AverageOrderAmount { get; set; }
            public int OrderCount { get; set; }
		}

		private class Order
		{
			public string Id { get; set; }
			public string CustomerId { get; set; }
			public decimal Amount { get; set; }
		}

		private class OrderResults
		{
			public string CustomerId { get; set; }
			public decimal TotalAmount { get; set; }
			public int OrderCount { get; set; }
			public decimal AverageOrderAmount { get; set; }            
		}

		private class OrdersAverageAmount : AbstractIndexCreationTask<Order, OrderResults>
		{
			public OrdersAverageAmount()
			{
				Map = orders => from order in orders
								select new
								{
									order.CustomerId,
									OrderCount = 1,
									TotalAmount = order.Amount,
									AverageOrderAmount = 0,
                                    Blah_Blah = 5.0m
								};

				Reduce = results => from result in results
									group result by result.CustomerId
										into g
										let amount = g.Sum(x=>x.TotalAmount)
										let count = g.Sum(x=>x.OrderCount)
										select new
										{
											CustomerId = g.Key,
                                            OrderCount = count,
                                            TotalAmount = amount,
											AverageOrderAmount = amount / count,
                                            Blah_Blah = 5.0m
										};
			}
		}

		[Fact]
		public void AverageOrderAmountShouldBeCalculatedCorrectly()
		{
            var indexPropsSetup = new IndexedPropertiesSetupDoc
            {
                DocumentKey = "CustomerId",
                FieldNameMappings =
				{
                    {"TotalAmount", "TotalAmount"},
					{"AverageOrderAmount", "AverageOrderAmount"},
                    {"OrderCount", "OrderCount"}
				}
            };

            RunIndexedProperties(indexPropsSetup);
		}

        [Fact]
        public void AverageOrderAmountShouldBeCalculatedCorrectly_WithScripting()
        {
            var indexPropsSetup = new IndexedPropertiesSetupDoc
            {
                 DocumentKey = "CustomerId",
                 Script = @"
// 'this' is the source doc, i.e. 'customer/1', 
// 'metadata' is the metadata of the source doc
// 'result' is the Lucene doc that is the output of the Map/Reduce indexing
var document = LoadDocument(result.CustomerId);
document.TotalAmount = result.TotalAmount;
document.AverageOrderAmount = result.AverageOrderAmount;
document.OrderCount = result.OrderCount;
document['@metadata'].Foo = 'whatever';
PutDocument(result.CustomerId, document);
", 
                 CleanupScript = @"
output(deleteDocId);
var document = LoadDocument(deleteDocId);
delete document.TotalAmount;
delete document.AverageOrderAmount;
delete document.OrderCount;
PutDocument(deleteDocId, document);
"
            };

            RunIndexedProperties(indexPropsSetup);
        }

        private void RunIndexedProperties(IndexedPropertiesSetupDoc indexPropsSetup)
        {
            using (var store = NewDocumentStore())
            {
                var ordersAverageAmount = new OrdersAverageAmount();
                ordersAverageAmount.Execute(store);

                store.DatabaseCommands.Put(IndexedPropertiesSetupDoc.IdPrefix + ordersAverageAmount.IndexName,
                                           null, RavenJObject.FromObject(indexPropsSetup), new RavenJObject());

                using (var session = store.OpenSession())
                {
                    session.Store(new Customer { Id = "customers/1", Name = "Customer 1" });
                    session.Store(new Customer { Id = "customers/2", Name = "Customer 2" });

                    session.Store(new Order { Id = "orders/1", CustomerId = "customers/1", Amount = 10 });
                    session.Store(new Order { Id = "orders/2", CustomerId = "customers/1", Amount = 5 });
                    session.Store(new Order { Id = "orders/3", CustomerId = "customers/1", Amount = 3 });

                    session.Store(new Order { Id = "orders/4", CustomerId = "customers/2", Amount = 1 });
                    session.Store(new Order { Id = "orders/5", CustomerId = "customers/2", Amount = 2 });

                    session.SaveChanges();
                }

                WaitForIndexing(store);                

                using (var session = store.OpenSession())
                {
                    var rawDoc1Test = store.DatabaseCommands.Get("customers/1").DataAsJson;
                    var rawDoc2Test = store.DatabaseCommands.Get("customers/2").DataAsJson;

                    var customer1 = session.Load<Customer>("customers/1");
                    var customer2 = session.Load<Customer>("customers/2");                    

                    Assert.Equal(6m, customer1.AverageOrderAmount);
                    Assert.Equal(3, customer1.OrderCount);
                    Assert.Equal(18m, customer1.TotalAmount);

                    Assert.Equal(1.5m, customer2.AverageOrderAmount);
                    Assert.Equal(2, customer2.OrderCount);
                    Assert.Equal(3, customer2.TotalAmount);                    
                }

                // now delete one of the source docs, "orders/4" and see if the results are correct
                store.DatabaseCommands.Delete("orders/4", null);
                WaitForIndexing(store);                

                using (var session = store.OpenSession())
                {
                    var customer1 = session.Load<Customer>("customers/1");
                    var customer2 = session.Load<Customer>("customers/2");

                    Assert.Equal(6m, customer1.AverageOrderAmount);
                    Assert.Equal(3, customer1.OrderCount);
                    Assert.Equal(18m, customer1.TotalAmount);

                    Assert.Equal(2.0m, customer2.AverageOrderAmount);
                    Assert.Equal(1, customer2.OrderCount);
                    Assert.Equal(2m, customer2.TotalAmount);
                }                

                // now delete "orders/5" and see if customers/2 has all its results removed
                store.DatabaseCommands.Delete("orders/5", null);
                WaitForIndexing(store);                                

                using (var session = store.OpenSession())
                {
                    var customer1 = session.Load<Customer>("customers/1");
                    var customer2 = session.Load<Customer>("customers/2");

                    Assert.Equal(6m, customer1.AverageOrderAmount);
                    Assert.Equal(3, customer1.OrderCount);
                    Assert.Equal(18m, customer1.TotalAmount);                    
                }

                // ensure the delete behaviour, i.e. the fields that were mapped are removed
                var rawDoc1 = store.DatabaseCommands.Get("customers/1").DataAsJson;
                var rawDoc2 = store.DatabaseCommands.Get("customers/2").DataAsJson;
                Assert.False(rawDoc2.ContainsKey("OrderCount"));
                Assert.False(rawDoc2.ContainsKey("AverageOrderAmount"));
                Assert.False(rawDoc2.ContainsKey("TotalAmount"));

                using (var session = store.OpenSession())
                {
                    session.Store(new Order { Id = "orders/9", CustomerId = "customers/1", Amount = 2 });
                    session.Store(new Order { Id = "orders/4", CustomerId = "customers/2", Amount = 4.8m });
                    session.Store(new Order { Id = "orders/5", CustomerId = "customers/2", Amount = 2.5m });
                    
                    session.SaveChanges();
                }

                WaitForIndexing(store);                

                using (var session = store.OpenSession())
                {
                    var customer1 = session.Load<Customer>("customers/1");
                    var customer2 = session.Load<Customer>("customers/2");

                    Assert.Equal(5m, customer1.AverageOrderAmount);
                    Assert.Equal(4, customer1.OrderCount);
                    Assert.Equal(20m, customer1.TotalAmount);

                    Assert.Equal(3.65m, customer2.AverageOrderAmount);
                    Assert.Equal(2, customer2.OrderCount);
                    Assert.Equal(7.3m, customer2.TotalAmount);
                }
            }
        }

		protected override void ModifyConfiguration(Database.Config.RavenConfiguration configuration)
		{
			configuration.Settings.Add("Raven/ActiveBundles", "IndexedProperties");
		}
	}
}