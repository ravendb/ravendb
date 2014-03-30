using System;
using System.Net;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestPlatform.UnitTestFramework;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Indexing;
using Raven.Client.Extensions;
using Raven.Tests.WinRT.Entities;

namespace Raven.Tests.WinRT
{
	[TestClass]
	public class AsyncLuceneQueryTests : RavenTestBase
	{
		public class User
		{
			public string Name { get; set; }
		}

		[TestMethod]
		public async Task CanQueryUsingAsyncSession()
		{
			var dbname = GenerateNewDatabaseName("AsyncLuceneQueryTests.CanQueryUsingAsyncSession");
			using (var store = NewDocumentStore())
			{
				await store.AsyncDatabaseCommands.Admin.EnsureDatabaseExistsAsync(dbname);

				using (var session = store.OpenAsyncSession(dbname))
				{
					await session.StoreAsync(new User {Name = "Ayende"});
					await session.SaveChangesAsync();
				}

				using (var session = store.OpenAsyncSession(dbname))
				{
					var users = await session.Advanced.AsyncLuceneQuery<User>()
					                         .WhereEquals("Name", "Ayende")
					                         .ToListAsync();

					Assert.AreEqual("Ayende", users.Item2[0].Name);
				}
			}
		}

		[TestMethod]
		public async Task IncludingARelatedEntityShouldAvoidHittingTheServerTwice()
		{
			var dbname = GenerateNewDatabaseName("AsyncLuceneQueryTests.IncludingARelatedEntityShouldAvoidHittingTheServerTwice");
			using (var store = NewDocumentStore())
			{
				store.Conventions.AllowQueriesOnId = true;

				await store.AsyncDatabaseCommands.Admin.EnsureDatabaseExistsAsync(dbname);

				var customer = new Customer {Name = "Customer #1", Id = "customer/1", Email = "someone@customer.com"};
				var order = new Order {Id = "orders/1", Note = "Hello", Customer = new DenormalizedReference {Id = customer.Id, Name = customer.Name}};
				using (var session = store.OpenAsyncSession(dbname))
				{
					await session.StoreAsync(customer);
					await session.StoreAsync(order);
					await session.SaveChangesAsync();
				}

				using (var session = store.OpenAsyncSession(dbname))
				{
					var orders = await session.Advanced.AsyncLuceneQuery<Order>()
					                          .Include(x => x.Customer.Id)
					                          .WhereEquals("Id", "orders/1")
					                          .ToListAsync();

					Assert.AreEqual("Hello", orders.Item2[0].Note);

					// NOTE: this call should not hit the server 
					var load = await session.LoadAsync<Customer>(customer.Id);

					Assert.AreEqual(1, session.Advanced.NumberOfRequests);
				}
			}
		}

		[TestMethod]
		public async Task QueryingAgainstANonindexedFieldRaisesAnException()
		{
			var dbname = GenerateNewDatabaseName("AsyncLuceneQueryTests.QueryingAgainstANonindexedFieldRaisesAnException");
			using (var store = NewDocumentStore())
			{
				await store.AsyncDatabaseCommands.Admin.EnsureDatabaseExistsAsync(dbname);

				using (var session = store.OpenAsyncSession(dbname))
				{
					await session.StoreAsync(new Customer {Name = "Customer #1", Id = "customer/1", Email = "someone@customer.com"});
					await session.SaveChangesAsync();
				}

				await store.AsyncDatabaseCommands
				                      .ForDatabase(dbname)
				                      .PutIndexAsync("Test", new IndexDefinition
				                      {
					                      Map = "from doc in docs.Companies select new { doc.Name }"
				                      }, true);

				var indexQuery = new IndexQuery { Query = "NonIndexedField:0" };
				for (int i = 0; i < 50; i++)
				{
					try
					{
						var query = await store.AsyncDatabaseCommands
						                       .ForDatabase(dbname)
						                       .QueryAsync("Test", indexQuery, null);

						if (query.IsStale)
						{
							await TaskEx.Delay(100);
							continue;
						}
					}
					catch (AggregateException e)
					{
						var actualException = e.ExtractSingleInnerException();
						Assert.ThrowsException<ErrorResponseException>(() => { throw actualException; });
						Assert.IsTrue(actualException.Message.Contains("System.ArgumentException: The field 'NonIndexedField' is not indexed, cannot query on fields that are not indexed"));
						return;
					}

					break;
				}
				Assert.Fail("Expected to get an exception");
			}
		}
	}
}