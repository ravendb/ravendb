using System.Threading.Tasks;
using Raven.Client.Extensions;
using Xunit;

namespace Raven.Tests.Core
{
	public class AsyncDatabaseCommandsTests : RavenTestBase
	{
		[Fact]
		public async Task CanGetDocumentsAsync()
		{
			var dbname = GenerateNewDatabaseName();
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
				Assert.Equal(3, documents.Length);
			}
		}

		[Fact]
		public async Task CanGetAListOfDatabasesAsync()
		{
			var dbname = GenerateNewDatabaseName();
			using (var store = NewDocumentStore())
			{
				await store.AsyncDatabaseCommands.Admin.EnsureDatabaseExistsAsync(dbname);

				var names = await store.AsyncDatabaseCommands.GetDatabaseNamesAsync(25);
				Assert.Contains(dbname, names);
			}
		}
	}
}