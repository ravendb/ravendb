using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;
using Microsoft.Silverlight.Testing;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Indexing;
using Raven.Client.Document;
using Raven.Client.Extensions;
using Raven.Tests.Silverlight.Entities;

namespace Raven.Tests.Silverlight
{
	public class AsyncLuceneQueryTests : RavenTestBase
	{
		public class User
		{
			public string Name { get; set; }
		}

		[Asynchronous]
		public IEnumerable<Task> CanQueryUsingAsyncSession()
		{
			var dbname = GenerateNewDatabaseName();
			using (var documentStore = new DocumentStore {Url = Url + Port}.Initialize())
			{
				yield return documentStore.AsyncDatabaseCommands.GlobalAdmin.EnsureDatabaseExistsAsync(dbname);

				using (var s = documentStore.OpenAsyncSession(dbname))
				{
					yield return s.StoreAsync(new User { Name = "Ayende" });
					yield return s.SaveChangesAsync();
				}

				using (var s = documentStore.OpenAsyncSession(dbname))
				{
					var queryResultAsync = s.Advanced.AsyncLuceneQuery<User>()
						.WhereEquals("Name", "Ayende")
						.ToListAsync();

					yield return queryResultAsync;

					Assert.AreEqual("Ayende", queryResultAsync.Result[0].Name);
				}
			}
		}

		[Asynchronous]
		public IEnumerable<Task> IncludingARelatedEntityShouldAvoidHittingTheServerTwice()
		{
			var dbname = GenerateNewDatabaseName();
			using (var documentStore = new DocumentStore {Url = Url + Port}.Initialize())
			{
				documentStore.Conventions.AllowQueriesOnId = true;

				yield return documentStore.AsyncDatabaseCommands.GlobalAdmin.EnsureDatabaseExistsAsync(dbname);

				var customer = new Customer {Name = "Customer #1", Id = "customer/1", Email = "someone@customer.com"};
				var order = new Order {Id = "orders/1", Note = "Hello", Customer = new DenormalizedReference {Id = customer.Id, Name = customer.Name}};
				using (var session = documentStore.OpenAsyncSession(dbname))
				{
					yield return session.StoreAsync(customer);
					yield return session.StoreAsync(order);
					yield return session.SaveChangesAsync();
				}

				using (var session = documentStore.OpenAsyncSession(dbname))
				{
					var query = session.Advanced.AsyncLuceneQuery<Order>()
						.Include(x => x.Customer.Id)
						.WhereEquals("Id", "orders/1")
						.ToListAsync();
					yield return query;

					Assert.AreEqual("Hello", query.Result[0].Note);

					// NOTE: this call should not hit the server 
					var load = session.LoadAsync<Customer>(customer.Id);
					yield return load;

					Assert.AreEqual(1, session.Advanced.NumberOfRequests);
				}
			}
		}

		[Asynchronous]
		public IEnumerable<Task> QueryingAgainstANonindexedFieldRaisesAnException()
		{
			var dbname = GenerateNewDatabaseName();
			using (var documentStore = new DocumentStore {Url = Url + Port}.Initialize())
			{
				yield return documentStore.AsyncDatabaseCommands.GlobalAdmin.EnsureDatabaseExistsAsync(dbname);

				using (var session = documentStore.OpenAsyncSession(dbname))
				{
					yield return session.StoreAsync(new Customer { Name = "Customer #1", Id = "customer/1", Email = "someone@customer.com" });
					yield return session.SaveChangesAsync();
				}

				var task = documentStore.AsyncDatabaseCommands
					.ForDatabase(dbname)
					.PutIndexAsync("Test", new IndexDefinition
					                       	{
					                       		Map = "from doc in docs.Companies select new { doc.Name }"
					                       	}, true);
				yield return (task);

				Task<QueryResult> query = null;
				var indexQuery = new IndexQuery {Query = "NonIndexedField:0"};
				for (int i = 0; i < 50; i++)
				{
					query = documentStore.AsyncDatabaseCommands
					                     .ForDatabase(dbname)
					                     .QueryAsync("Test", indexQuery, null);
					yield return (query);

					if (query.Exception != null)
					{
						Assert.IsInstanceOfType(query.Exception.ExtractSingleInnerException(), typeof (ErrorResponseException));
						yield break;
					}

					if (query.Result.IsStale)
					{
						yield return TaskEx.Delay(100);
						continue;
					}
					yield break;
				}
				Assert.Fail("Expected to get an exception");
			}
		}
	}
}
