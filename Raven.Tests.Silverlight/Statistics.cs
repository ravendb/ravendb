namespace Raven.Tests.Silverlight
{
	using System.Collections.Generic;
	using System.Threading.Tasks;
	using Client.Document;
	using Client.Extensions;
	using Document;
	using Microsoft.Silverlight.Testing;
	using Microsoft.VisualStudio.TestTools.UnitTesting;

	public class Statistics : RavenTestBase
	{
		[Asynchronous]
		public IEnumerable<Task> Can_retrieve_statistics_for_the_default_database()
		{
			var store = new DocumentStore {Url = Url + Port};
			store.Initialize();

			var getStats = store.AsyncDatabaseCommands.GetStatisticsAsync();
			yield return getStats;

			Assert.IsNotNull(getStats.Result);
			Assert.AreNotEqual(0, getStats.Result.Indexes.Length);
		}

		[Asynchronous]
		public IEnumerable<Task> Can_retrieve_statistics_for_a_database()
		{
			var dbname = GenerateNewDatabaseName();
			var store = new DocumentStore {Url = Url + Port};
			store.Initialize();
			yield return store.AsyncDatabaseCommands.EnsureDatabaseExistsAsync(dbname);

			var getStats = store.AsyncDatabaseCommands.ForDatabase(dbname).GetStatisticsAsync();
			yield return getStats;

			var stats = getStats.Result;
			Assert.AreEqual(0, stats.CountOfDocuments);
			Assert.AreEqual(2, stats.CountOfIndexes); // we expect two indexes in a brand new db (for now)
		}

		[Asynchronous]
		public IEnumerable<Task> Statistics_should_not_be_cached()
		{
			var dbname = GenerateNewDatabaseName();
			var store = new DocumentStore {Url = Url + Port};
			store.Initialize();
			yield return store.AsyncDatabaseCommands.EnsureDatabaseExistsAsync(dbname);

			var getStats = store.AsyncDatabaseCommands.ForDatabase(dbname).GetStatisticsAsync();
			yield return getStats;

			Assert.AreEqual(0, getStats.Result.CountOfDocuments);

			using (var session = store.OpenAsyncSession(dbname))
			{
				session.Store(new Company {Name = "Change the Stats, Inc."});
				yield return session.SaveChangesAsync();
			}

			var verifyStats = store.AsyncDatabaseCommands.ForDatabase(dbname).GetStatisticsAsync();
			yield return verifyStats;

			Assert.AreEqual(1, verifyStats.Result.CountOfDocuments);
		}
	}
}