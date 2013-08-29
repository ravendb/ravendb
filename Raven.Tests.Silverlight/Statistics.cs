using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Silverlight.Testing;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Raven.Abstractions.Indexing;
using Raven.Client.Document;
using Raven.Client.Extensions;
using Raven.Tests.Document;

namespace Raven.Tests.Silverlight
{
	public class Statistics : RavenTestBase
	{
		[Asynchronous]
		public IEnumerable<Task> CanRetrieveStatisticsForTheDefaultDatabase()
		{
			using (var documentStore = new DocumentStore {Url = Url + Port}.Initialize())
			{
				var getStats = documentStore.AsyncDatabaseCommands
					.GetStatisticsAsync();
				yield return getStats;

				Assert.IsNotNull(getStats.Result);
			}
		}

		[Asynchronous]
		public IEnumerable<Task> CanRetrieveStatisticsForADatabase()
		{
			var dbname = GenerateNewDatabaseName();
			using (var documentStore = new DocumentStore {Url = Url + Port}.Initialize())
			{
				yield return documentStore.AsyncDatabaseCommands.GlobalAdmin.EnsureDatabaseExistsAsync(dbname);

				yield return documentStore.AsyncDatabaseCommands.ForDatabase(dbname).PutIndexAsync("test", new IndexDefinition
				                                                                                           	{
				                                                                                           		Map = "from doc in docs select new { doc.Name}"
				                                                                                           	}, true);

				var getStats = documentStore.AsyncDatabaseCommands.ForDatabase(dbname).GetStatisticsAsync();
				yield return getStats;

				var stats = getStats.Result;
				Assert.AreEqual(0, stats.CountOfDocuments);
				Assert.IsTrue(stats.CountOfIndexes > 0);
			}
		}

		[Asynchronous]
		public IEnumerable<Task> StatisticsShouldNotBeCached()
		{
			var dbname = GenerateNewDatabaseName();
			using (var documentStore = new DocumentStore {Url = Url + Port}.Initialize())
			{
				yield return documentStore.AsyncDatabaseCommands.GlobalAdmin.EnsureDatabaseExistsAsync(dbname);

				var getStats = documentStore.AsyncDatabaseCommands.ForDatabase(dbname).GetStatisticsAsync();
				yield return getStats;

				Assert.AreEqual(0, getStats.Result.CountOfDocuments);

				using (var session = documentStore.OpenAsyncSession(dbname))
				{
					yield return session.StoreAsync(new Company { Name = "Change the Stats, Inc." });
					yield return session.SaveChangesAsync();
				}

				var verifyStats = documentStore.AsyncDatabaseCommands.ForDatabase(dbname).GetStatisticsAsync();
				yield return verifyStats;

				Assert.AreEqual(1, verifyStats.Result.CountOfDocuments);
			}
		}
	}
}