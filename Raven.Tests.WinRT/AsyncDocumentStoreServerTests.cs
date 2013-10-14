using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestPlatform.UnitTestFramework;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Client.Extensions;
using Raven.Json.Linq;
using Raven.Tests.Document;

namespace Raven.Tests.WinRT
{
	[TestClass]
	public class AsyncDocumentStoreServerTests : RavenTestBase
	{
		[TestMethod]
		public async Task CanInsertAsyncAndMultiGetAsync()
		{
			var dbname = GenerateNewDatabaseName("AsyncDocumentStoreServerTests.CanInsertAsyncAndMultiGetAsync");
			using (var store = NewDocumentStore())
			{
				await store.AsyncDatabaseCommands.Admin.EnsureDatabaseExistsAsync(dbname);

				var entity1 = new Company {Name = "Async Company #1"};
				var entity2 = new Company {Name = "Async Company #2"};
				using (var session_for_storing = store.OpenAsyncSession(dbname))
				{
					await session_for_storing.StoreAsync(entity1);
					await session_for_storing.StoreAsync(entity2);
					await session_for_storing.SaveChangesAsync();
				}

				using (var session_for_loading = store.OpenAsyncSession(dbname))
				{
					var companies = await session_for_loading.LoadAsync<Company>(new[] {entity1.Id, entity2.Id});

					Assert.AreEqual(entity1.Name, companies[0].Name);
					Assert.AreEqual(entity2.Name, companies[1].Name);
				}
			}
		}

		[TestMethod]
		public async Task CanInsertAsyncAndLoadAsync()
		{
			var dbname = GenerateNewDatabaseName("AsyncDocumentStoreServerTests.CanInsertAsyncAndLoadAsync");
			using (var store = NewDocumentStore())
			{
				await store.AsyncDatabaseCommands.Admin.EnsureDatabaseExistsAsync(dbname);

				var entity = new Company {Name = "Async Company #1"};
				using (var session_for_storing = store.OpenAsyncSession(dbname))
				{
					await session_for_storing.StoreAsync(entity);
					await session_for_storing.SaveChangesAsync();
				}

				using (var session_for_loading = store.OpenAsyncSession(dbname))
				{
					var company = await session_for_loading.LoadAsync<Company>(entity.Id);
					Assert.AreEqual(entity.Name, company.Name);
				}
			}
		}

		[TestMethod]
		public async Task CanInsertAsyncAndDeleteAsync()
		{
			var dbname = GenerateNewDatabaseName("AsyncDocumentStoreServerTests.CanInsertAsyncAndDeleteAsync");
			using (var store = NewDocumentStore())
			{
				await store.AsyncDatabaseCommands.Admin.EnsureDatabaseExistsAsync(dbname);

				var entity = new Company {Name = "Async Company #1", Id = "companies/1"};
				using (var session = store.OpenAsyncSession(dbname))
				{
					await session.StoreAsync(entity);
					await session.SaveChangesAsync();
				}

				using (var for_loading = store.OpenAsyncSession(dbname))
				{
					var company = await for_loading.LoadAsync<Company>(entity.Id);
					Assert.IsNotNull(company);
				}

				using (var for_deleting = store.OpenAsyncSession(dbname))
				{
					var company = await for_deleting.LoadAsync<Company>(entity.Id);
					for_deleting.Delete(company);
					await for_deleting.SaveChangesAsync();
				}

				using (var for_verifying = store.OpenAsyncSession(dbname))
				{
					var company = await for_verifying.LoadAsync<Company>(entity.Id);
					Assert.IsNull(company);
				}
			}
		}

		[TestMethod]
		public async Task CanQueryByIndex()
		{
			var dbname = GenerateNewDatabaseName("AsyncDocumentStoreServerTests.CanQueryByIndex");
			using (var store = NewDocumentStore())
			{
				await store.AsyncDatabaseCommands.Admin.EnsureDatabaseExistsAsync(dbname);

				var entity = new Company {Name = "Async Company #1", Id = "companies/1"};
				using (var session = store.OpenAsyncSession(dbname))
				{
					await session.StoreAsync(entity);
					await session.SaveChangesAsync();
				}

				await store.AsyncDatabaseCommands
				           .ForDatabase(dbname)
				           .PutIndexAsync("Test", new IndexDefinition
				           {
					           Map = "from doc in docs.Companies select new { doc.Name }"
				           }, true);


				for (int i = 0; i < 50; i++)
				{
					var query = await store.AsyncDatabaseCommands
					                       .ForDatabase(dbname)
					                       .QueryAsync("Test", new IndexQuery(), null);

					if (query.IsStale)
					{
						await TaskEx.Delay(100);
						continue;
					}

					Assert.AreNotEqual(0, query.TotalResults);
					break;
				}
			}
		}

		[TestMethod]
		public async Task CanProjectValueFromCollection()
		{
			var dbname = GenerateNewDatabaseName("AsyncDocumentStoreServerTests.CanProjectValueFromCollection");
			using (var store = NewDocumentStore())
			{
				await store.AsyncDatabaseCommands.Admin.EnsureDatabaseExistsAsync(dbname);

				using (var session = store.OpenAsyncSession(dbname))
				{
					await session.StoreAsync(new Company
					{
						Name = "Project Value Company",
						Contacts = new List<Contact>
						{
							new Contact {Surname = "Abbot"},
							new Contact {Surname = "Costello"}
						}
					});
					await session.SaveChangesAsync();


					QueryResult query;
					do
					{
						query = await store.AsyncDatabaseCommands
						                   .ForDatabase(dbname)
						                   .QueryAsync("dynamic",
						                               new IndexQuery
						                               {
							                               FieldsToFetch = new[] {"Contacts,Surname"}
						                               },
						                               new string[0]);
						if (query.IsStale)
							await TaskEx.Delay(100);
					} while (query.IsStale);

					var ravenJToken = (RavenJArray) query.Results[0]["Contacts"];
					Assert.AreEqual(2, ravenJToken.Count());
					Assert.AreEqual("Abbot", ravenJToken[0].Value<string>("Surname"));
					Assert.AreEqual("Costello", ravenJToken[1].Value<string>("Surname"));
				}
			}
		}
	}
}