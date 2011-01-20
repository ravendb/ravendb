namespace Raven.Tests.Silverlight
{
	using System.Collections.Generic;
	using System.Threading.Tasks;
	using Client.Document;
	using Client.Extensions;
	using Microsoft.Silverlight.Testing;
	using Xunit;

	public class AsyncLuceneQueryTests : RavenTestBase
	{
		public class User
		{
			public string Name { get; set; }
		}

		[Asynchronous]
		public IEnumerable<Task> Can_query_using_async_session()
		{
			var dbname = GenerateNewDatabaseName();
			var documentStore = new DocumentStore {Url = Url + Port};
			documentStore.Initialize();
			yield return documentStore.AsyncDatabaseCommands.EnsureDatabaseExistsAsync(dbname);

			using (var s = documentStore.OpenAsyncSession(dbname))
			{
				s.Store(new User {Name = "Ayende"});
				yield return s.SaveChangesAsync();
			}

			using (var s = documentStore.OpenAsyncSession(dbname))
			{
				var queryResultAsync = s.Advanced.AsyncLuceneQuery<User>()
					.WhereEquals("Name", "Ayende")
					.QueryResultAsync;

				yield return queryResultAsync;

				Assert.Equal("Ayende", queryResultAsync.Result.Results[0].Value<string>("Name"));
			}
		}

		[Asynchronous]
		public IEnumerable<Task> Including_a_related_entity_should_avoid_hitting_the_server_twice()
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
				var query = session.Advanced.AsyncLuceneQuery<Order>()
					.Include(x => x.Customer.Id)
					.WhereEquals("Id", "orders/1")
					.QueryResultAsync;
				yield return query;

				Assert.Equal("Hello", query.Result.Results[0].Value<string>("Note"));

				// NOTE: this call should not hit the server 
				var load = session.LoadAsync<Customer>(customer.Id);
				yield return load;

				Assert.Equal(1, session.Advanced.NumberOfRequests);
			}
		}
	}
}