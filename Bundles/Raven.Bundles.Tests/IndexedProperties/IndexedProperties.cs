using System;
using System.ComponentModel.Composition.Hosting;
using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Bundles.IndexedProperties;
using Raven.Client;
using Raven.Client.Embedded;
using Raven.Client.IndexedProperties;
using Raven.Client.Indexes;
using Raven.Client.Linq;

namespace Raven.Bundles.Tests.IndexedProperties
{
	public class IndexedProperty
	{
		public void RunTest()
		{
			using (var store = new EmbeddableDocumentStore())
			{
				store.Configuration.RunInMemory = true;
				store.Configuration.InitialNumberOfItemsToIndexInSingleBatch = 50;
				//This lets us wire up the Trigger (only works in embedded mode though)
				store.Configuration.Container = new CompositionContainer(new TypeCatalog(typeof(IndexedPropertiesTrigger)));
				store.Initialize();
				IndexCreation.CreateIndexes(typeof(Orders_ByCustomer_Count).Assembly, store);
				CreateDocumentsByEntityNameIndex(store);

				ExecuteTest(store);
			}
		}

		private void ExecuteTest(IDocumentStore store)
		{
			var customer1 = new Customer { Name = "Matt", Country = "UK" };
			var customer2 = new Customer { Name = "Debs", Country = "UK", };
			var customer3 = new Customer { Name = "Bob", Country = "USA" };

			using (var session = store.OpenSession())
			{
				session.Store(customer1);
				session.Store(customer2);
				session.Store(customer3);
				session.SaveChanges();

				Console.WriteLine("Saved Customer docs: {0}",
					String.Join(", ", new[] { customer1.Id, customer2.Id, customer3.Id }));
			}

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

			var setupDoc = new SetupDoc
			{
				//This is the name of the field in the Map/Reduce results that holds to Id of 
				//the documents that we need to write the values back into
				DocumentKey = "CustomerId",

				//This contains the mappings from the Map/Reduce result back to the original doc
				FieldNameMappings = new []
				{
					Tuple.Create("Average", "AverageOrderCost"),
					Tuple.Create("Count", "NumberOfOrders")
				}
			};

			var indexName = typeof(Orders_ByCustomer_Count).Name.Replace("_", "/");
			using (var session = store.OpenSession())
			{
				session.Store(setupDoc, SetupDoc.IdPrefix + indexName);
				session.SaveChanges();
			}

			var testJson = store.DatabaseCommands.Get(SetupDoc.IdPrefix + indexName);
			var testJsonTxt = testJson.DataAsJson.ToString();

			WaitForIndexToUpdate(store, indexName);

			using (var session = store.OpenSession())
			{
				var docs = session.Load<Customer>(new[] { customer1.Id, customer2.Id, customer3.Id });
				foreach (var doc in docs)
				{
					Console.WriteLine("Reading back doc \"{0}\": Total Orders {1}, Average Cost {2}",
									doc.Id, doc.NumberOfOrders, doc.AverageOrderCost);
				}

				var test = session.Load<SetupDoc>("Raven/IndexedProperties/" + indexName);
			}

			Console.WriteLine("Deleting orders \"{0}\" and \"{1}\"", order1.Id, order8.Id);
			store.DatabaseCommands.Delete(order1.Id, null); //delete 1 of the orders for customer1
			store.DatabaseCommands.Delete(order8.Id, null); //delete 1 of the orders for customer3

			WaitForIndexToUpdate(store, indexName);

			var customerJsonTest = store.DatabaseCommands.Get(customer1.Id);
			var docJsonTest = store.DatabaseCommands.Get(order2.Id);

			using (var session = store.OpenSession())
			{
				var docs = session.Load<Customer>(new[] { customer1.Id, customer2.Id, customer3.Id });
				foreach (var doc in docs)
				{
					Console.WriteLine("Reading back doc \"{0}\": Total Orders {1}, Average Cost {2}", 
									doc.Id, doc.NumberOfOrders, doc.AverageOrderCost);
				}
			}

			Console.WriteLine("Test completed, press <ENTER> to exit");
			Console.ReadLine();
		}

		private void WaitForIndexToUpdate(IDocumentStore store, string indexName)
		{
			using (var session = store.OpenSession())
			{
				//Just issue a query to ensure that it's not stale
				RavenQueryStatistics stats;
				Console.WriteLine("Issuing query to ensure the indexing has completed.....");
				var customers = session.Query<Orders_ByCustomer_Count.CustomerResult>(indexName)
					.Customize(s => s.WaitForNonStaleResultsAsOfNow())
					.Statistics(out stats)
					.ToList();
				Console.WriteLine("DONE, index is no longer stale, contains {0} items:\n\t{1}", 
					customers.Count, String.Join("\n\t", customers.Select(x => new { x.Count, x.Average, x.TotalCost })));
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
