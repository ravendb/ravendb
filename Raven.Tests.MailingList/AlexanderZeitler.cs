using System.Linq;
using Raven.Client;
using Raven.Client.Embedded;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.MailingList
{
	public class AlexanderZeitler : RavenTest
	{
		public class Order
		{
			public string Id { get; set; }
			public string CustomerId { get; set; }
			public string[] SupplierIds { get; set; }
			public double TotalPrice { get; set; }
		}

		public class Customer
		{
			public string Id { get; set; }
			public string Name { get; set; }
			public string Address { get; set; }
			public short Age { get; set; }
			public string HashedPassword { get; set; }
		}

		[Fact]
		public void It_should_be_found()
		{
			var customer = new Customer()
			{
				Name = "ALFKI"
			};

			Order order = null;

			// red
			using (IDocumentStore documentStore = new EmbeddableDocumentStore()
			{
				RunInMemory = true
			}.Initialize())
			{

				new RavenDocumentsByEntityName().Execute(documentStore);

				// green
				//var documentStore = new DocumentStore()
				//{
				//    Url = "http://localhost:8080/databases/ravensubdocs"
				//}.Initialize();

				//documentStore.DatabaseCommands.DeleteByIndex("Raven/DocumentsByEntityName", new IndexQuery());


				using (var session = documentStore.OpenSession())
				{
					session.Store(customer);
					session.SaveChanges();
					order = new Order()
					{
						CustomerId = customer.Id,
						TotalPrice = 200
					};

					session.Store(order);
					session.SaveChanges();
				}

				using (var session = documentStore.OpenSession())
				{
					order = session.Include<Order>(x => x.CustomerId)
						.Load(order.Id);

					// this will not require querying the server!
					customer = session.Load<Customer>(order.CustomerId);
					Assert.NotEqual(null, customer);
					Assert.Equal("ALFKI", customer.Name);
				}


				using (var session = documentStore.OpenSession())
				{
					var orders = session.Query<Order>()
						.Customize(x => x.Include<Order>(o => o.CustomerId))
						// also try to comment this
						.Where(x => x.TotalPrice > 100)
						.ToList();

					Assert.Equal(1, orders.Count);

					foreach (var order1 in orders)
					{
						// this will not require querying the server!
						var cust = session.Load<Customer>(order1.CustomerId);
						Assert.NotEqual(null, cust);
					}
				}
			}
		}
	}
}