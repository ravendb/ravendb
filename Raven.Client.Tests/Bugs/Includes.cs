using System;
using System.Linq;
using Raven.Client.Document;
using Raven.Database.Indexing;
using Raven.Server;
using Xunit;

namespace Raven.Client.Tests.Bugs
{
	public class Includes : RemoteClientTest, IDisposable
	{
		private readonly IDocumentStore store;
		private readonly RavenDbServer server;

		public Includes()
		{
			server = GetNewServer(8080, GetPath(DbName));

			store = new DocumentStore
			{
				Url = "http://localhost:8080"
			}.Initialize();

			store.DatabaseCommands.PutIndex("Orders/ByName",
			                                new IndexDefinition
			                                {
												Map = "from doc in docs.Orders select new { doc.Name }"
			                                });

			using(var session = store.OpenSession())
			{
				
				for (int i = 0; i < 15; i++)
				{
					var customer = new Customer
					{
						Email = "ayende@ayende.com",
						Name = "Oren"
					};
					
					session.Store(customer);

					session.Store(new Order
					{
						Customer = new DenormalizedReference
						{
							Id = customer.Id,
							Name = customer.Name
						},
						Name = "MyOrder #" + (i + 1)
					});
				}
				session.SaveChanges();
			}
		}

		[Fact]
		public void CanIncludeWithSingleLoad()
		{
			using (var session = store.OpenSession())
			{
				var order = session
					.Include("Customer.Id")
					.Load<Order>("orders/1");

				Assert.Equal(1, session.NumberOfRequests);

				var customer = session.Load<Customer>(order.Customer.Id);

				Assert.NotNull(customer);

				Assert.Equal(1, session.NumberOfRequests);
			}
		}

		[Fact]
		public void CanIncludeWithQuery()
		{
			using (var session = store.OpenSession())
			{
				var orders = session
					.LuceneQuery<Order>("Orders/ByName")
					.WaitForNonStaleResults()
					.Include("Customer.Id")
					.Take(2)
					.ToList();

				Assert.Equal(1, session.NumberOfRequests);

				var customer1 = session.Load<Customer>(orders[0].Customer.Id);
				var customer2 = session.Load<Customer>(orders[1].Customer.Id);

				Assert.NotNull(customer1);
				Assert.NotNull(customer2);

				Assert.NotSame(customer1, customer2);

				Assert.Equal(1, session.NumberOfRequests);
			}
		}

        [Fact]
        public void CanIncludeExtensionWithQuery() {
            using (var session = store.OpenSession()) {
                var orders = session
                    .LuceneQuery<Order>("Orders/ByName")
                    .WaitForNonStaleResults()
                    .Include(o => o.Customer.Id)
                    .Take(2)
                    .ToList();

                Assert.Equal(1, session.NumberOfRequests);

                var customer1 = session.Load<Customer>(orders[0].Customer.Id);
                var customer2 = session.Load<Customer>(orders[1].Customer.Id);

                Assert.NotNull(customer1);
                Assert.NotNull(customer2);

                Assert.NotSame(customer1, customer2);

                Assert.Equal(1, session.NumberOfRequests);
            }
        }

		[Fact]
		public void CanIncludeWithMultiLoad()
		{
			using (var session = store.OpenSession())
			{
				var orders = session
					.Include("Customer.Id")
					.Load<Order>("orders/1", "orders/2");

				Assert.Equal(1, session.NumberOfRequests);

				var customer1 = session.Load<Customer>(orders[0].Customer.Id);
				var customer2 = session.Load<Customer>(orders[1].Customer.Id);

				Assert.NotNull(customer1);
				Assert.NotNull(customer2);

				Assert.NotSame(customer1, customer2);

				Assert.Equal(1, session.NumberOfRequests);
			}
		}

		public void Dispose()
		{
			store.Dispose();
			server.Dispose();
		}

		public class Order
		{
			public string Id { get; set; }
			public string Name { get; set; }
			public DenormalizedReference Customer { get; set; }
		}

		public class DenormalizedReference
		{
			public string Id { get; set; }
			public string Name { get; set; }
		}

		public class Customer
		{
			public string Id { get; set; }
			public string Name { get; set; }
			public string Email { get; set; }

		}
	}
}