using System;
using System.Collections.Generic;
using System.ComponentModel.Composition.Hosting;
using System.Linq;
using System.Text;
using Raven.Abstractions.Indexing;
using Raven.Bundles.IndexedProperties;
using Raven.Client;
using Raven.Client.Embedded;
using Raven.Client.Indexes;
using Raven.Client.Linq;

namespace Raven.Bundles.Tests.IndexedProperties
{
	public class IndexedProperty //: Raven.Tests.RavenTest
	{
		public class Dummy
		{
			public bool Boolean { get; set; }
			public Dummy Object { get; set; }
		}

		public void RunTest()
		{
			using (var store = new EmbeddableDocumentStore())
			{
				store.Configuration.RunInMemory = true;
				store.Configuration.Container = new CompositionContainer(new TypeCatalog(typeof(IndexedPropertiesTrigger)));
				store.Initialize();
				IndexCreation.CreateIndexes(typeof(Customers_ByOrder_Count).Assembly, store);
				CreateDocumentsByEntityNameIndex(store);

				ExecuteTest(store);
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

		private void ExecuteTest(IDocumentStore store)
		{
			var customer1 = new Customer
			{
				Name = "Matt",
				Country = "UK",
				Orders = new[]
			{
				new Order {Cost = 9.99m},
				new Order {Cost = 12.99m},
				new Order {Cost = 1.25m}
			}
			};
			var customer2 = new Customer
			{
				Name = "Debs",
				Country = "UK",
				Orders = new[]
			{
				new Order {Cost = 99.99m},
				new Order {Cost = 105.99m}
			}
			};
			var customer3 = new Customer
			{
				Name = "Bob",
				Country = "USA",
				Orders = new[]
			{
				new Order {Cost = 0.05m},
				new Order {Cost = 0.99m},
				new Order {Cost = 1.99m}
			}
			};

			using (var session = store.OpenSession())
			{
				session.Store(customer1);
				session.Store(customer2);
				session.Store(customer3);
				session.SaveChanges();
				Console.WriteLine("Save docs: {0}, {1}, {2}", customer1.Id, customer2.Id, customer3.Id);
			}

			//Proposed Config doc structure (from https://groups.google.com/d/msg/ravendb/Ik6Iv96Z_3I/PXs7h-hawpEJ)
			//DocId = Raven/IndexedProperties/Orders/AveragePurchaseAmount
			// "Orders/AveragePurchaseAmount" from the doc key is the index name we get the data from
			//{ 
			//   //The field name that gives us the docId of the doc to write to (is this right???)
			//  "DocumentKey": "CustomerId",
			//
			//  //mapping from index field to doc field (to store it in)
			//  "Properties": [
			//       "AveragePurchaseAmount": "AveragePurchaseAmount" 
			//  ]
			//}

			//The whole idea is so we can do this query 
			//using the AverageValue taken from the Map/Reduce index and stored in the doc
			//Country:UK SortBy:AveragePurchaseAmount desc

			using (var session = store.OpenSession())
			{
				//Just issue a query to ensure that it's not stale
				RavenQueryStatistics stats;
				Console.WriteLine("Issuing query");
				var customers = session.Query<CustomerResult>("Customers/ByOrder/Count")
					.Customize(s => s.WaitForNonStaleResultsAsOfNow())
					.Statistics(out stats)
					.ToList();
				var totalOrders = customers.Count;

				foreach (var id in new[] { customer1.Id, customer2.Id })
				{
					var customerEx = session.Advanced.DatabaseCommands.Get(id);
					Console.WriteLine("\n\nReading back calculated Average from doc[{0}] = {1}", id, customerEx.DataAsJson["AverageOrderCost"]);
				}

				//Would like to be able to do it like this, but the doc was stored as Customer
				//so we can't load it as CustomerEx (even though that's the shape of the Json)
				//var cutstomerEx = session.Load<CustomerEx>(customer1.Id);
			}

			Console.WriteLine("Deleting doc \"{0}\"", customer3.Id);
			store.DatabaseCommands.Delete(customer3.Id, null);

			Console.WriteLine("Test completed, press <ENTER> to exit");
			Console.ReadLine();
		}
	}

	public class Customers_ByOrder_Count : AbstractIndexCreationTask<Customer, CustomerResult>
	{
		public Customers_ByOrder_Count()
		{
			Map = customers => from customer in customers
							   from order in customer.Orders
							   select new { CustomerId = customer.Id, TotalCost = order.Cost, Count = 1, Average = 0 };
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
	}

	public class CustomerResult
	{
		public String CustomerId { get; set; }
		public Decimal TotalCost { get; set; }
		public int Count { get; set; }
		public double Average { get; set; }
	}

	public class Customer
	{
		public String Id { get; set; }
		public String Name { get; set; }
		public String Country { get; set; }
		public IList<Order> Orders { get; set; }
	}

	public class CustomerEx : Customer
	{
		public Decimal AverageOrderCost { get; set; }
		public Decimal NumberOfOrders { get; set; }
	}

	public class Order
	{
		public String Id { get; set; }
		public Decimal Cost { get; set; }
	}
}
