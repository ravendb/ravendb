extern alias client;
using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Embedded;
using Raven.Client.Indexes;
using Raven.Client.Linq;
using Xunit;

namespace Raven.Bundles.Tests.IndexedProperties
{
	public class IndexedProperty
	{
		[Fact]
		public void Can_Use_Indexed_Properties()
		{
			using (var store = new EmbeddableDocumentStore())
			{
				store.Configuration.RunInMemory = true;
				//This lets us wire up the Trigger (only works in embedded mode though)
				store.Configuration.Settings["Raven/ActiveBundles"] = "IndexedProperties";
				store.Configuration.PostInit();
				store.Initialize();
				new Orders_ByCustomer_Count().Execute(store);	
				//Create another index, so we can check we use the index specified in the SetupDoc
				CreateDocumentsByEntityNameIndex(store);

				ExecuteTest(store);
			}
		}

		private void ExecuteTest(IDocumentStore store)
		{
			var customer1 = new Customer {Name = "Matt", Country = "UK"};
			var customer2 = new Customer {Name = "Debs", Country = "UK"};
			var customer3 = new Customer {Name = "Bob", Country = "USA"};

			using (var session = store.OpenSession())
			{
				session.Store(customer1);
				session.Store(customer2);
				session.Store(customer3);
				session.SaveChanges();
			}
			var customerDocIds = new[] {customer1.Id, customer2.Id, customer3.Id};

			//Orders for Customer1
			var order1 = new Order {CustomerId = customer1.Id, Cost = 9.99m};
			var order2 = new Order {CustomerId = customer1.Id, Cost = 12.99m};
			var order3 = new Order {CustomerId = customer1.Id, Cost = 1.25m};
			//Orders for Customer2
			var order4 = new Order {CustomerId = customer2.Id, Cost = 99.99m};
			var order5 = new Order {CustomerId = customer2.Id, Cost = 105.99m};
			//Orders for Customer3
			var order6 = new Order {CustomerId = customer3.Id, Cost = 0.05m};
			var order7 = new Order {CustomerId = customer3.Id, Cost = 0.99m};
			var order8 = new Order {CustomerId = customer3.Id, Cost = 1.99m};

			using (var session = store.OpenSession())
			{
				session.Store(order1);
				session.Store(order2);
				session.Store(order3);

				session.Store(order4);
				session.Store(order5);

				session.Store(order6);
				session.Store(order7);
				session.Store(order8);

				session.SaveChanges();
			}

			var setupDoc = new IndexedPropertiesSetupDoc
			{
				//This is the name of the field in the Map/Reduce results that holds to Id of 
				//the documents that we need to write the values back into
				DocumentKey = "CustomerId",

				//This contains the mappings from the Map/Reduce result back to the original doc
				FieldNameMappings = 
				{
					{"Average", "AverageOrderCost"},
					{"Count", "NumberOfOrders"}
				}
			};

			var indexName = new Orders_ByCustomer_Count().IndexName;
			using (var session = store.OpenSession())
			{
				session.Store(setupDoc, IndexedPropertiesSetupDoc.IdPrefix + indexName);
				session.SaveChanges();
			}
			WaitForIndexToUpdate(store, indexName);

			//Test 1 - Check that the averages are written back to the customer docs
			var customerDocs = GetCustomerDocs(store, customerDocIds);
			Assert.Equal((order1.Cost + order2.Cost + order3.Cost)/3.0m, customerDocs[0].AverageOrderCost, 10);
			Assert.Equal(3, customerDocs[0].NumberOfOrders);
			Assert.Equal((order4.Cost + order5.Cost)/2.0m, customerDocs[1].AverageOrderCost, 10);
			Assert.Equal(2, customerDocs[1].NumberOfOrders);
			Assert.Equal((order6.Cost + order7.Cost + order8.Cost)/3.0m, customerDocs[2].AverageOrderCost, 10);
			Assert.Equal(3, customerDocs[2].NumberOfOrders);

			//Test 2 - delete some of the orders for 2 of the customers and check the averages update
			store.DatabaseCommands.Delete(order1.Id, null);
			store.DatabaseCommands.Delete(order8.Id, null);
			WaitForIndexToUpdate(store, indexName);

			customerDocs = GetCustomerDocs(store, customerDocIds);
			Assert.Equal((order2.Cost + order3.Cost)/2.0m, customerDocs[0].AverageOrderCost, 10);
			Assert.Equal(2, customerDocs[0].NumberOfOrders);
			Assert.Equal((order4.Cost + order5.Cost)/2.0m, customerDocs[1].AverageOrderCost, 10);
			Assert.Equal(2, customerDocs[1].NumberOfOrders);
			Assert.Equal((order6.Cost + order7.Cost)/2.0m, customerDocs[2].AverageOrderCost, 10);
			Assert.Equal(2, customerDocs[2].NumberOfOrders);

			//Test 3 - Delete all the orders for customer1, it now has NO orders!
			store.DatabaseCommands.Delete(order2.Id, null);
			store.DatabaseCommands.Delete(order3.Id, null);
			WaitForIndexToUpdate(store, indexName);

			var customer1Json = store.DatabaseCommands.Get(customer1.Id);
			Assert.False(customer1Json.DataAsJson.ContainsKey("AverageOrderCost"));
			Assert.False(customer1Json.DataAsJson.ContainsKey("NumberOfOrders"));

			//Test 4 - Modify the orders for customer2, it's average should update
			using (var session = store.OpenSession())
			{
				var orders = session.Load<Order>(new[] {order4.Id, order5.Id});
				orders[0].Cost = 75.8m;
				orders[1].Cost = 26.1m;
				session.SaveChanges();
			}
			WaitForIndexToUpdate(store, indexName);

			customerDocs = GetCustomerDocs(store, customerDocIds);
			Assert.Equal((75.8m + 26.1m)/2.0m, customerDocs[1].AverageOrderCost, 10);
			Assert.Equal(2, customerDocs[1].NumberOfOrders);

			//Test 5 - Can actually do the queries we'd like to, i.e. query against customers using the AverageOrderCost field
			using (var session = store.OpenSession())
			{
				RavenQueryStatistics stats;
				var customerByTotalOrderCose = session.Query<Customer>()
					.OrderBy(x => x.AverageOrderCost)
					.Customize(x => x.WaitForNonStaleResults())
					.Statistics(out stats)
					.ToList();
				var previous = Decimal.MinValue;
				foreach (var customer in customerByTotalOrderCose)
				{
					Assert.True(customer.AverageOrderCost > previous);
					previous = customer.AverageOrderCost;
				}
			}
		}

		private IList<Customer> GetCustomerDocs(IDocumentStore store, string[] customerDocIds)
		{
			using (var session = store.OpenSession())
			{
				return session.Load<Customer>(customerDocIds);
			}
		}

		private void WaitForIndexToUpdate(IDocumentStore store, string indexName)
		{
			using (var session = store.OpenSession())
			{
				//Just issue a query to ensure that it's not stale
				RavenQueryStatistics stats;
				var customerResults = session.Query<Orders_ByCustomer_Count.CustomerResult>(indexName)
					.Customize(s => s.WaitForNonStaleResultsAsOfNow())
					.Statistics(out stats)
					.ToList();
			}
		}

		private void CreateDocumentsByEntityNameIndex(EmbeddableDocumentStore store)
		{
			var database = store.DocumentDatabase;
			if (database.GetIndexDefinition("Raven/DocumentsByEntityName") == null)
			{
				database.PutIndex("Raven/DocumentsByEntityName", new IndexDefinition
				{
					Map =
						@"from doc in docs 
let Tag = doc[""@metadata""][""Raven-Entity-Name""]
select new { Tag, LastModified = (DateTime)doc[""@metadata""][""Last-Modified""] };",
					Indexes =
					{
						{"Tag", FieldIndexing.NotAnalyzed},
					},
					Stores =
					{
						{"Tag", FieldStorage.No},
						{"LastModified", FieldStorage.No}
					}
				});
			}
		}
	}

	public class Orders_ByCustomer_Count : AbstractIndexCreationTask<Order, Orders_ByCustomer_Count.CustomerResult>
	{
		public Orders_ByCustomer_Count()
		{
			Map = orders => from order in orders
							   select new
							   {
									CustomerId = order.CustomerId, 
									TotalCost = order.Cost, 
									Count = 1, 
									Average = 0
							   };
			Reduce = results => from result in results
								group result by new { result.CustomerId }
									into g
									select new
									{
										g.Key.CustomerId,
										TotalCost = g.Sum(x => x.TotalCost),
										Count = g.Sum(x => x.Count),
										Average = g.Sum(x => x.TotalCost) / g.Sum(x => x.Count),
									};
		}

		public class CustomerResult
		{
			public string CustomerId { get; set; }
			public Decimal TotalCost { get; set; }
			public int Count { get; set; }
			public Decimal Average { get; set; }
		}
	}

	public class Customer
	{
		public String Id { get; set; }
		public String Name { get; set; }
		public String Country { get; set; }
		public Decimal AverageOrderCost { get; set; }
		public int NumberOfOrders { get; set; }
	}	

	public class Order
	{
		public String Id { get; set; }
		public String CustomerId { get; set; }
		public Decimal Cost { get; set; }
	}
}
