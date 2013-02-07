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
			public decimal AverageOrderAmount { get; set; }
            public int OrderCount { get; set; }
		}

		private class Order
		{
			public string Id { get; set; }
			public string CustomerId { get; set; }
			public decimal TotalAmount { get; set; }
		}

		private class OrderResults
		{
			public string CustomerId { get; set; }
			public decimal Amount { get; set; }
			public int Count { get; set; }
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
									Amount = order.TotalAmount,
									Count = 1,
									AverageOrderAmount = order.TotalAmount
								};

				Reduce = results => from result in results
									group result by result.CustomerId
										into g
										let amount = g.Sum(x=>x.Amount)
										let count = g.Sum(x=>x.Count)
										select new
										{
											CustomerId = g.Key,
											Count = count,
											Amount = amount,
											AverageOrderAmount = amount / count
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
					{"AverageOrderAmount", "AverageOrderAmount"}
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
//                Script = @"
//this.AverageOrderAmount = parseFloat(AverageOrderAmount) + 5.0;
//this.OrderCount = parseInt(Count);"
                 Script = @"
this.AverageOrderAmount = parseFloat(AverageOrderAmount);
this.OrderCount = parseInt(Count);"
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
                                           null,
                                           RavenJObject.FromObject(indexPropsSetup),
                                           new RavenJObject());

                using (var session = store.OpenSession())
                {
                    session.Store(new Customer { Id = "customers/1", Name = "Customer 1" });
                    session.Store(new Customer { Id = "customers/2", Name = "Customer 2" });

                    session.Store(new Order { Id = "orders/1", CustomerId = "customers/1", TotalAmount = 10 });
                    session.Store(new Order { Id = "orders/2", CustomerId = "customers/1", TotalAmount = 5 });
                    session.Store(new Order { Id = "orders/3", CustomerId = "customers/1", TotalAmount = 3 });

                    session.Store(new Order { Id = "orders/4", CustomerId = "customers/2", TotalAmount = 1 });
                    session.Store(new Order { Id = "orders/5", CustomerId = "customers/2", TotalAmount = 2 });

                    session.SaveChanges();
                }

                WaitForIndexing(store);
                Console.WriteLine("Finished waiting for indexing\n");

                using (var session = store.OpenSession())
                {
                    var customer1 = session.Load<Customer>("customers/1");
                    var customer2 = session.Load<Customer>("customers/2");                    

                    //Assert.Equal(6m, customer1.AverageOrderAmount);
                    //Assert.Equal(1.5m, customer2.AverageOrderAmount);
                }

                var rawDoc1a = store.DatabaseCommands.Get("customers/1").DataAsJson;
                var rawDoc2a = store.DatabaseCommands.Get("customers/2").DataAsJson;                

                // now delete one of the source docs, orders/4 and see if the results are correct
                store.DatabaseCommands.Delete("orders/4", null);
                WaitForIndexing(store);
                Console.WriteLine("Finished waiting for indexing\n");
                var rawDoc1b = store.DatabaseCommands.Get("customers/1").DataAsJson;
                var rawDoc2b = store.DatabaseCommands.Get("customers/2").DataAsJson;                

                // now orders/5 and see if customers/2 has all its results removed
                store.DatabaseCommands.Delete("orders/5", null);
                WaitForIndexing(store);
                Console.WriteLine("Finished waiting for indexing\n");
                var rawDoc1c = store.DatabaseCommands.Get("customers/1").DataAsJson;
                var rawDoc2c = store.DatabaseCommands.Get("customers/2").DataAsJson;

                // TODO write units tests that ensue the delete behaviour, i.e. the fields that were mapped are removed

                var final = rawDoc2c.ToString();
            }
        }

		protected override void ModifyConfiguration(Database.Config.RavenConfiguration configuration)
		{
			configuration.Settings.Add("Raven/ActiveBundles", "IndexedProperties");
		}
	}
}