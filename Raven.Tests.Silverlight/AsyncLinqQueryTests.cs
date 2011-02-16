namespace Raven.Tests.Silverlight
{
	using System.Collections.Generic;
	using System.Linq;
	using System.Threading.Tasks;
	using Client.Document;
	using Client.Extensions;
	using Client.Linq;
	using Document;
	using Entities;
	using Microsoft.Silverlight.Testing;
	using Microsoft.VisualStudio.TestTools.UnitTesting;

	public class AsyncLinqQueryTests : RavenTestBase
	{
		[Asynchronous][Ignore]
		//[ExpectedException(typeof(NotSupportedException))]
		public IEnumerable<Task> Calling_ToList_raises_an_exception()
		{
			var dbname = GenerateNewDatabaseName();
			var documentStore = new DocumentStore { Url = Url + Port };
			documentStore.Initialize();
			yield return documentStore.AsyncDatabaseCommands.EnsureDatabaseExistsAsync(dbname);

			using (var session = documentStore.OpenAsyncSession(dbname))
			{
				//NOTE: shouldn't compile
				//var query = session.Query<Company>()
				//            .Where(x => x.Name == "Doesn't Really Matter")
				//            .ToList();

				// should compile
				var list = new List<string>().ToList();
			}
		}

		[Asynchronous]
		public IEnumerable<Task> Can_perform_a_simple_where()
		{
			var dbname = GenerateNewDatabaseName();
			var documentStore = new DocumentStore { Url = Url + Port };
			documentStore.Initialize();
			yield return documentStore.AsyncDatabaseCommands.EnsureDatabaseExistsAsync(dbname);

			var entity = new Company { Name = "Async Company #1", Id = "companies/1" };
			using (var session = documentStore.OpenAsyncSession(dbname))
			{
				session.Store(entity);
				yield return session.SaveChangesAsync();
			}

			using (var session = documentStore.OpenAsyncSession(dbname))
			{
			var query = session.Query<Company>()
						.Where(x => x.Name == "Async Company #1")
						.ToListAsync();
				yield return query;

				Assert.AreEqual(1, query.Result.Count);
				Assert.AreEqual("Async Company #1", query.Result[0].Name);
			}
		}

		[Asynchronous]
		public IEnumerable<Task> Can_test_two_conditions_in_a_where_clause()
		{
			var dbname = GenerateNewDatabaseName();
			var documentStore = new DocumentStore { Url = Url + Port };
			documentStore.Initialize();
			yield return documentStore.AsyncDatabaseCommands.EnsureDatabaseExistsAsync(dbname);

			using (var session = documentStore.OpenAsyncSession(dbname))
			{
				session.Store(new Company { Name = "Async Company", Phone = 55555, Id = "companies/1" });
				session.Store(new Company { Name = "Async Company", Phone = 12345, Id = "companies/2" });
				yield return session.SaveChangesAsync();
			}

			using (var session = documentStore.OpenAsyncSession(dbname))
			{
				var query = session.Query<Company>()
							.Where(x => x.Name == "Async Company" && x.Phone == 12345)
							.ToListAsync();
				yield return query;

				Assert.AreEqual(1, query.Result.Count);
				Assert.AreEqual(12345, query.Result[0].Phone);
			}
		}

		[Asynchronous]
		public IEnumerable<Task> Can_query_on_not_equal()
		{
			var dbname = GenerateNewDatabaseName();
			var documentStore = new DocumentStore { Url = Url + Port };
			documentStore.Initialize();
			yield return documentStore.AsyncDatabaseCommands.EnsureDatabaseExistsAsync(dbname);

			using (var s = documentStore.OpenAsyncSession(dbname))
			{
				s.Store(new Company { Name = "Ayende" });
				s.Store(new Company { Name = "Oren" });
				yield return s.SaveChangesAsync();
			}

			using (var s = documentStore.OpenAsyncSession(dbname))
			{
				var query = s.Query<Company>()
					.Where(x => x.Name != "Oren")
					.ToListAsync();
				yield return query;

				Assert.AreEqual(1, query.Result.Count);
			}
		}

		[Asynchronous]
		public IEnumerable<Task> Can_perform_an_order_by()
		{
			var dbname = GenerateNewDatabaseName();
			var documentStore = new DocumentStore { Url = Url + Port };
			documentStore.Initialize();
			yield return documentStore.AsyncDatabaseCommands.EnsureDatabaseExistsAsync(dbname);

			using (var session = documentStore.OpenAsyncSession(dbname))
			{
				session.Store(new Company { Name = "Moon Dog", Id = "companies/1" });
				session.Store(new Company { Name = "Alpha Dog", Id = "companies/2" });
				session.Store(new Company { Name = "Loony Lin", Id = "companies/3" });
				session.Store(new Company { Name = "Betty Boop", Id = "companies/4" });
				yield return session.SaveChangesAsync();
			}

			using (var session = documentStore.OpenAsyncSession(dbname))
			{
				var query = session.Query<Company>()
							.OrderBy(x=>x.Name)
							.ToListAsync();
				yield return query;

				Assert.AreEqual(4, query.Result.Count);
				Assert.AreEqual("Alpha Dog", query.Result[0].Name);
				Assert.AreEqual("Betty Boop", query.Result[1].Name);
				Assert.AreEqual("Loony Lin", query.Result[2].Name);
			}
		}

		[Asynchronous]
		public IEnumerable<Task> Can_perform_a_where_starts_with()
		{
			var dbname = GenerateNewDatabaseName();
			var documentStore = new DocumentStore { Url = Url + Port };
			documentStore.Initialize();
			yield return documentStore.AsyncDatabaseCommands.EnsureDatabaseExistsAsync(dbname);

			using (var session = documentStore.OpenAsyncSession(dbname))
			{
				session.Store(new Company { Name = "Async Company #1", Id = "companies/1" });
				session.Store(new Company { Name = "Async Company #2", Id = "companies/2" });
				session.Store(new Company { Name = "Different Company", Id = "companies/3" });
				yield return session.SaveChangesAsync();
			}

			using (var session = documentStore.OpenAsyncSession(dbname))
			{
				var query = session.Query<Company>()
							.Where(x => x.Name.StartsWith("Async"))
							.ToListAsync();
				yield return query;

				Assert.AreEqual(2, query.Result.Count);
			}
		}

		[Asynchronous]
		public IEnumerable<Task> Can_perform_an_include_in_a_linq_query()
		{
			var dbname = GenerateNewDatabaseName();
			var documentStore = new DocumentStore { Url = Url + Port };
			documentStore.Initialize();
			yield return documentStore.AsyncDatabaseCommands.EnsureDatabaseExistsAsync(dbname);

			var customer = new Customer { Name = "Customer #1", Id = "customer/1", Email = "someone@customer.com" };
			var order = new Order { Id = "orders/1", Note = "Hello", Customer = new DenormalizedReference { Id = customer.Id, Name = customer.Name } };
			using (var session = documentStore.OpenAsyncSession(dbname))
			{
				session.Store(customer);
				session.Store(order);
				yield return session.SaveChangesAsync();
			}

			using (var session = documentStore.OpenAsyncSession(dbname))
			{
				var query = session.Query<Order>()
							.Include(x => x.Customer.Id)
							.Where(x => x.Id == "orders/1")
							.ToListAsync();
				yield return query;

				Assert.AreEqual("Hello", query.Result[0].Note);

				// NOTE: this call should not hit the server 
				var load = session.LoadAsync<Customer>(customer.Id);
				yield return load;

				Assert.AreEqual(1, session.Advanced.NumberOfRequests);
			}
		}

		[Asynchronous][Ignore]
		public IEnumerable<Task> Can_perform_a_projection_in_a_linq_query()
		{
			var dbname = GenerateNewDatabaseName();
			var documentStore = new DocumentStore { Url = Url + Port };
			documentStore.Initialize();
			yield return documentStore.AsyncDatabaseCommands.EnsureDatabaseExistsAsync(dbname);

			var entity = new Company { Name = "Async Company #1", Id = "companies/1" };
			using (var session = documentStore.OpenAsyncSession(dbname))
			{
				session.Store(entity);
				yield return session.SaveChangesAsync();
			}

			using (var session = documentStore.OpenAsyncSession(dbname))
			{
				var query = session.Query<Company>()
							.Where(x => x.Name == "Async Company #1")
							.AsProjection<TheCompanyName>()
							.ToListAsync();
				yield return query;

				//NOTE: it seems that the fields from the projection are not proprogated to the query,
				//		 this manifests as a problem casting the type, because (since it does see the projected fields)
				//		 it assumes that the must be the original entity (i.e., Company)

				Assert.AreEqual(1, query.Result.Count);
				Assert.AreEqual("Async Company #1", query.Result[0].Name);
			}
		}

		[Asynchronous]
		public IEnumerable<Task> Can_perform_an_any()
		{
			var dbname = GenerateNewDatabaseName();
			var documentStore = new DocumentStore { Url = Url + Port };
			documentStore.Initialize();
			yield return documentStore.AsyncDatabaseCommands.EnsureDatabaseExistsAsync(dbname);

			using (var session = documentStore.OpenAsyncSession(dbname))
			{
				session.Store(new Order
				{
					Id = "orders/1",
					Lines = new List<OrderLine>{ new OrderLine{Quantity = 1}, new OrderLine{Quantity = 2} }
				});
				session.Store(new Order
				{
					Id = "orders/2",
					Lines = new List<OrderLine> { new OrderLine { Quantity = 1 }, new OrderLine { Quantity = 2 } }
				});
				session.Store(new Order
				{
					Id = "orders/3",
					Lines = new List<OrderLine> { new OrderLine { Quantity = 1 }, new OrderLine { Quantity = 1 } }

				});

				yield return session.SaveChangesAsync();
			}

			using (var session = documentStore.OpenAsyncSession(dbname))
			{
				var query = session.Query<Order>()
							.Where( x => x.Lines.Any( line => line.Quantity > 1 ))
							.ToListAsync();
				yield return query;

				Assert.AreEqual(2, query.Result.Count);
			}
		}

		[Asynchronous]
		public IEnumerable<Task> Can_send_a_linq_query_as_a_string()
		{
			var dbname = GenerateNewDatabaseName();
			var documentStore = new DocumentStore { Url = Url + Port };
			documentStore.Initialize();
			yield return documentStore.AsyncDatabaseCommands.EnsureDatabaseExistsAsync(dbname);

			var entity = new Company { Name = "Async Company #1", Id = "companies/1" };
			using (var session = documentStore.OpenAsyncSession(dbname))
			{
				session.Store(entity);
				yield return session.SaveChangesAsync();
			}

			using (var session = documentStore.OpenAsyncSession(dbname))
			{
				var linq = @"from doc in docs where doc.Name == ""Async Company #1"" select doc";
				
				var query = session.Advanced.AsyncDatabaseCommands
					.LinearQueryAsync(linq,0,25);
				yield return query;

				Assert.AreEqual(1, query.Result.Results.Count);
			}
		}
	}
}