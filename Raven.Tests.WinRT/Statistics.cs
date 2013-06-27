using System.Threading.Tasks;
using Microsoft.VisualStudio.TestPlatform.UnitTestFramework;
using Raven.Abstractions.Indexing;
using Raven.Client.Extensions;
using Raven.Tests.Document;

namespace Raven.Tests.WinRT
{
	[TestClass]
	public class Statistics : RavenTestBase
	{
		[TestMethod]
		public async Task CanRetrieveStatisticsForTheDefaultDatabase()
		{
			using (var store = NewDocumentStore())
			{
				var stats = await store.AsyncDatabaseCommands.GetStatisticsAsync();
				Assert.IsNotNull(stats);
			}
		}

		[TestMethod]
		public async Task CanRetrieveStatisticsForADatabase()
		{
			var dbname = GenerateNewDatabaseName("Statistics.CanRetrieveStatisticsForADatabase");
			using (var store = NewDocumentStore())
			{
				await store.AsyncDatabaseCommands.Admin.EnsureDatabaseExistsAsync(dbname);

				await store.AsyncDatabaseCommands.ForDatabase(dbname).PutIndexAsync("test", new IndexDefinition
				{
					Map = "from doc in docs select new { doc.Name}"
				}, true);

				var stats = await store.AsyncDatabaseCommands.ForDatabase(dbname).GetStatisticsAsync();
				Assert.AreEqual(0, stats.CountOfDocuments);
				Assert.IsTrue(stats.CountOfIndexes > 0);
			}
		}

		[TestMethod]
		public async Task StatisticsShouldNotBeCached()
		{
			var dbname = GenerateNewDatabaseName("Statistics.StatisticsShouldNotBeCached");
			using (var store = NewDocumentStore())
			{
				await store.AsyncDatabaseCommands.Admin.EnsureDatabaseExistsAsync(dbname);

				var stats = await store.AsyncDatabaseCommands.ForDatabase(dbname).GetStatisticsAsync();

				Assert.AreEqual(0, stats.CountOfDocuments);

				using (var session = store.OpenAsyncSession(dbname))
				{
					await session.StoreAsync(new Company {Id = "companies/1", Name = "Change the Stats, Inc."});
					await session.SaveChangesAsync();
				}

				stats = await store.AsyncDatabaseCommands.ForDatabase(dbname).GetStatisticsAsync();

				Assert.AreEqual(1, stats.CountOfDocuments);
			}
		}
	}
}