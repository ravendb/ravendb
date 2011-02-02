namespace Raven.Tests.Silverlight
{
	using System.Linq;
	using System.Collections.Generic;
	using System.Threading.Tasks;
	using Client.Document;
	using Client.Extensions;
	using Database.Indexing;
	using Microsoft.Silverlight.Testing;
	using Microsoft.VisualStudio.TestTools.UnitTesting;

	public class AsyncDatabaseCommandsTests : RavenTestBase
	{
		[Asynchronous]
		public IEnumerable<Task> Can_get_index_names_async()
		{
			var dbname = GenerateNewDatabaseName();
			var documentStore = new DocumentStore { Url = Url + Port };
			documentStore.Initialize();
			yield return documentStore.AsyncDatabaseCommands.EnsureDatabaseExistsAsync(dbname);

			var task = documentStore.AsyncDatabaseCommands.ForDatabase(dbname).GetIndexNamesAsync(0, 25);
			yield return task;

			Assert.AreEqual("Raven/DocumentsByEntityName", task.Result[0]);
		}

		[Asynchronous]
		public IEnumerable<Task> Can_get_indexes_async()
		{
			var dbname = GenerateNewDatabaseName();
			var documentStore = new DocumentStore { Url = Url + Port };
			documentStore.Initialize();
			yield return documentStore.AsyncDatabaseCommands.EnsureDatabaseExistsAsync(dbname);

			var task = documentStore.AsyncDatabaseCommands.ForDatabase(dbname).GetIndexesAsync(0, 25);
			yield return task;

			Assert.AreEqual("Raven/DocumentsByEntityName", task.Result[0].Name);
		}

		[Asynchronous]
		public IEnumerable<Task> Can_put_an_index_async()
		{
			var dbname = GenerateNewDatabaseName();
			var documentStore = new DocumentStore { Url = Url + Port };
			documentStore.Initialize();
			yield return documentStore.AsyncDatabaseCommands.EnsureDatabaseExistsAsync(dbname);

			yield return documentStore.AsyncDatabaseCommands
				.ForDatabase(dbname)
				.PutIndexAsync("Test", new IndexDefinition
				{
					Map = "from doc in docs.Companies select new { doc.Name }"
				}, true);

			var verification = documentStore.AsyncDatabaseCommands
				.ForDatabase(dbname)
				.GetIndexNamesAsync(0, 25);
			yield return verification;

			Assert.IsTrue(verification.Result.Contains("Test"));
		}

		[Asynchronous]
		public IEnumerable<Task> Can_retrieve_statistics_for_a_server()
		{
			var documentStore = new DocumentStore { Url = Url + Port };
			documentStore.Initialize();

			var getStats = documentStore.AsyncDatabaseCommands.GetStatisticsAsync();
			yield return getStats;

			Assert.IsNotNull(getStats.Result);
			//TODO: What's the correct way to test this?
		}

		[Asynchronous]
		public IEnumerable<Task> Can_retrieve_statistics_for_a_database()
		{
			var dbname = GenerateNewDatabaseName();
			var documentStore = new DocumentStore { Url = Url + Port };
			documentStore.Initialize();
			yield return documentStore.AsyncDatabaseCommands.EnsureDatabaseExistsAsync(dbname);

			var getStats = documentStore.AsyncDatabaseCommands.ForDatabase(dbname).GetStatisticsAsync();
			yield return getStats;

			var stats = getStats.Result;
			Assert.AreEqual(0, stats.CountOfDocuments);
			Assert.AreEqual(1, stats.CountOfIndexes);
		}
	}
}