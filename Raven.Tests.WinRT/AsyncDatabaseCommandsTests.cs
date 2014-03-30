using System.Threading.Tasks;
using Microsoft.VisualStudio.TestPlatform.UnitTestFramework;
using Raven.Client.Extensions;
using Raven.Tests.Document;
using System.Linq;

namespace Raven.Tests.WinRT
{
	[TestClass]
	public class AsyncDatabaseCommandsTests : RavenTestBase
	{
		[TestMethod]
		public async Task CanGetDocumentsAsync()
		{
			var dbname = GenerateNewDatabaseName("AsyncDatabaseCommandsTests.CanGetDocumentsAsync");
			using (var store = NewDocumentStore())
			{
				await store.AsyncDatabaseCommands.Admin.EnsureDatabaseExistsAsync(dbname);

				using (var session = store.OpenAsyncSession(dbname))
				{
					await session.StoreAsync(new Company { Name = "Hai" });
					await session.StoreAsync(new Company { Name = "I can haz cheezburgr?" });
					await session.StoreAsync(new Company { Name = "lol" });
					await session.SaveChangesAsync();
				}

				var documents = await store.AsyncDatabaseCommands.ForDatabase(dbname).GetDocumentsAsync(0, 25);
				// 4 because we have also the HiLo document.
				Assert.AreEqual(4, documents.Length);
			}
		}

		[TestMethod]
		public async Task CanGetDocumentsWhoseIdStartsWithAPrefix()
		{
			var dbname = GenerateNewDatabaseName("AsyncDatabaseCommandsTests.CanGetDocumentsWhoseIdStartsWithAPrefix");
			using (var store = NewDocumentStore())
			{
				await store.AsyncDatabaseCommands.Admin.EnsureDatabaseExistsAsync(dbname);

				using (var session = store.OpenAsyncSession(dbname))
				{
					await session.StoreAsync(new Company {Name = "Something with the desired prefix"});
					await session.StoreAsync(new Contact { Surname = "Something without the desired prefix" });
					await session.SaveChangesAsync();
				}

				var documents = await store.AsyncDatabaseCommands.ForDatabase(dbname).StartsWithAsync("Companies", 0, 25);
				Assert.AreEqual(1, documents.Length);
			}
		}

		[TestMethod]
		public async Task CanGetAListOfDatabasesAsync()
		{
			var dbname = GenerateNewDatabaseName("AsyncDatabaseCommandsTests.CanGetAListOfDatabasesAsync");
			using (var store = NewDocumentStore())
			{
				await store.AsyncDatabaseCommands.Admin.EnsureDatabaseExistsAsync(dbname);

				var names = await store.AsyncDatabaseCommands.GetDatabaseNamesAsync(25);
				Assert.IsTrue(names.Contains(dbname));
			}
		}

		[TestMethod]
		public async Task ShouldNotCacheTheListOfDatabases()
		{
			var first = GenerateNewDatabaseName("AsyncDatabaseCommandsTests.ShouldNotCacheTheListOfDatabases1");
			using (var store = NewDocumentStore())
			{
				await store.AsyncDatabaseCommands.Admin.EnsureDatabaseExistsAsync(first);

				var names = await store.AsyncDatabaseCommands.GetDatabaseNamesAsync(25);
				Assert.IsTrue(names.Contains(first));

				var second = GenerateNewDatabaseName("AsyncDatabaseCommandsTests.ShouldNotCacheTheListOfDatabases2");
				await store.AsyncDatabaseCommands.Admin.EnsureDatabaseExistsAsync(second);

				var names2 = await store.AsyncDatabaseCommands.GetDatabaseNamesAsync(25);
				Assert.IsTrue(names2.Contains(second));
			}
		}

		[TestMethod]
		public async Task CanGetDeleteADocumentById()
		{
			var dbname = GenerateNewDatabaseName();
			using (var store = NewDocumentStore())
			{
				await store.AsyncDatabaseCommands.Admin.EnsureDatabaseExistsAsync(dbname);

				var entity = new Company {Name = "Async Company #1"};
				using (var session = store.OpenAsyncSession(dbname))
				{
					await session.StoreAsync(entity);
					await session.SaveChangesAsync();

					await store.AsyncDatabaseCommands.ForDatabase(dbname).DeleteDocumentAsync(entity.Id);
				}

				using (var for_verifying = store.OpenAsyncSession(dbname))
				{
					var company = await for_verifying.LoadAsync<Company>(entity.Id);
					Assert.IsNull(company);
				}
			}
		}

		[TestMethod]
		public async Task TheResponseForGettingDocumentsShouldNotBeCached()
		{
			var dbname = GenerateNewDatabaseName("AsyncDatabaseCommandsTests.TheResponseForGettingDocumentsShouldNotBeCached");
			using (var store = NewDocumentStore())
			{
				await store.AsyncDatabaseCommands.Admin.EnsureDatabaseExistsAsync(dbname);

				using (var session = store.OpenAsyncSession(dbname))
				{
					await session.StoreAsync(new Company {Name = "Hai"});
					await session.StoreAsync(new Company {Name = "I can haz cheezburgr?"});
					await session.StoreAsync(new Company {Name = "lol"});
					await session.SaveChangesAsync();
				}

				var documents = await store.AsyncDatabaseCommands.ForDatabase(dbname).GetDocumentsAsync(0, 25);
				Assert.AreEqual(4, documents.Length);

				await store.AsyncDatabaseCommands.ForDatabase(dbname).DeleteDocumentAsync(documents[0].Key);
				documents = await store.AsyncDatabaseCommands.ForDatabase(dbname).GetDocumentsAsync(0, 25);
				Assert.AreEqual(3, documents.Length);
			}
		}
	}
}