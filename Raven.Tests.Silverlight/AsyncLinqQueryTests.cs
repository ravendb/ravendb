namespace Raven.Tests.Silverlight
{
	using System;
	using System.Collections.Generic;
	using System.Linq;
	using System.Threading.Tasks;
	using Client.Document;
	using Client.Extensions;
	using Client.Linq;
	using Database.Data;
	using Database.Indexing;
	using Document;
	using Microsoft.Silverlight.Testing;
	using Microsoft.VisualStudio.TestTools.UnitTesting;
	using Assert = Xunit.Assert;

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

				Assert.Equal(1, query.Result.Count);
				Assert.Equal("Async Company #1", query.Result[0].Name);
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

				Assert.Equal(1, query.Result.Count);
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

				Assert.Equal(4, query.Result.Count);
				Assert.Equal("Alpha Dog", query.Result[0].Name);
				Assert.Equal("Betty Boop", query.Result[1].Name);
				Assert.Equal("Loony Lin", query.Result[2].Name);
			}
		}

		//[Asynchronous]
		//public IEnumerable<Task> Can_perform_an_include_in_a_linq_query_asychronously()
		//{
		//    var dbname = GenerateNewDatabaseName();
		//    var documentStore = new DocumentStore { Url = Url + Port };
		//    documentStore.Initialize();
		//    yield return documentStore.AsyncDatabaseCommands.EnsureDatabaseExistsAsync(dbname);

		//    var customer = new Customer { Name = "Customer #1", Id = "customer/1",Email = "someone@customer.com"};
		//    var order = new Order{BillingAddress = new Address(),ShippingAddress = new Address(),Customer = new DenormalizedReference{Id = customer.Id,Name = customer.Name}};
		//    using (var session = documentStore.OpenAsyncSession(dbname))
		//    {
		//        session.Store(customer);
		//        session.Store(order);
		//        yield return session.SaveChangesAsync();
		//    }

		//    using (var session = documentStore.OpenAsyncSession(dbname))
		//    {
		//        var query = session.Query<Order>()
		//                    .Where(x => x.Customer.Name == "Customer #1")
		//                    .ToListAsync();
		//        yield return query;

		//        Assert.Equal(1, query.Result.Count);
		//        //Assert.Equal("Async Company #1", query.Result[0].Name);
		//    }
		//}

		//[Asynchronous]
		//public IEnumerable<Task> Can_perform_a_projection_in_a_linq_query()
		//{
		//    var dbname = GenerateNewDatabaseName();
		//    var documentStore = new DocumentStore { Url = Url + Port };
		//    documentStore.Initialize();
		//    yield return documentStore.AsyncDatabaseCommands.EnsureDatabaseExistsAsync(dbname);

		//    var entity = new Company { Name = "Async Company #1", Id = "companies/1" };
		//    using (var session = documentStore.OpenAsyncSession(dbname))
		//    {
		//        session.Store(entity);
		//        yield return session.SaveChangesAsync();
		//    }

		//    using (var session = documentStore.OpenAsyncSession(dbname))
		//    {
		//        var query = session.Query<Company>()
		//                    .Where(x => x.Name == "Async Company #1")
		//                    .Select(x => x.Name)
		//                    .ToListAsync();
		//        yield return query;

		//        Assert.Equal(1, query.Result.Count);
		//        Assert.Equal("Async Company #1", query.Result[0]);
		//    }
		//}
	}

	public class Order
	{
		public string Id { get; set; }
		public string Note { get; set; }
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