using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Silverlight.Testing;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Raven.Abstractions.Indexing;
using Raven.Client.Document;
using Raven.Client.Extensions;

namespace Raven.Tests.Silverlight
{
	public class Indexes : RavenTestBase
	{
		[Asynchronous]
		public IEnumerable<Task> TaskIsFaultedWhenDeletingIndexFails()
		{
			using (var documentStore = new DocumentStore
			{
				Url = Url + Port,
				Conventions =
					{
						FailoverBehavior = FailoverBehavior.FailImmediately
					}
			}.Initialize())
			{
				var task = documentStore.AsyncDatabaseCommands.ForDatabase("NonExistent").DeleteIndexAsync("NonExistent");
				yield return task;

				Assert.IsTrue(task.IsFaulted);
			}
		}

		[Asynchronous]
		public IEnumerable<Task> CanGetIndexNamesAsync()
		{
			var dbname = GenerateNewDatabaseName();
			using (var documentStore = new DocumentStore {Url = Url + Port}.Initialize())
			{
				yield return documentStore.AsyncDatabaseCommands.EnsureDatabaseExistsAsync(dbname);
				yield return
					documentStore.AsyncDatabaseCommands.ForDatabase(dbname).PutIndexAsync("test", new IndexDefinition
					                                                                              	{
					                                                                              		Map = "from doc in docs select new { doc.Name}"
					                                                                              	}, true);
				var task = documentStore.AsyncDatabaseCommands.ForDatabase(dbname).GetIndexNamesAsync(0, 25);
				yield return task;

				Assert.IsTrue(task.Result.Any(x => x == "test"));

			}
		}

		[Asynchronous]
		public IEnumerable<Task> CanGetIndexesAsync()
		{
			var dbname = GenerateNewDatabaseName();
			using (var documentStore = new DocumentStore {Url = Url + Port}.Initialize())
			{
				yield return documentStore.AsyncDatabaseCommands.EnsureDatabaseExistsAsync(dbname);

				yield return
					documentStore.AsyncDatabaseCommands.ForDatabase(dbname).PutIndexAsync("test", new IndexDefinition
					                                                                              	{
					                                                                              		Map = "from doc in docs select new { doc.Name}"
					                                                                              	}, true);

				var task = documentStore.AsyncDatabaseCommands.ForDatabase(dbname).GetIndexesAsync(0, 25);
				yield return task;

				Assert.IsTrue(task.Result.Any(x => x.Name == "test"));
			}
		}

		[Asynchronous]
		public IEnumerable<Task> CanPutAnIndexAsync()
		{
			var dbname = GenerateNewDatabaseName();
			using (var documentStore = new DocumentStore {Url = Url + Port}.Initialize())
			{
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
		}

		[Asynchronous]
		public IEnumerable<Task> CanDeleteAnIndexAsync()
		{
			var dbname = GenerateNewDatabaseName();
			using (var documentStore = new DocumentStore {Url = Url + Port}.Initialize())
			{
				yield return documentStore.AsyncDatabaseCommands.EnsureDatabaseExistsAsync(dbname);

				yield return documentStore.AsyncDatabaseCommands
					.ForDatabase(dbname)
					.PutIndexAsync("Test", new IndexDefinition
					                       	{
					                       		Map = "from doc in docs.Companies select new { doc.Name }"
					                       	}, true);

				var verify_put = documentStore.AsyncDatabaseCommands
					.ForDatabase(dbname)
					.GetIndexNamesAsync(0, 25);
				yield return verify_put;

				Assert.IsTrue(verify_put.Result.Contains("Test"));

				yield return documentStore.AsyncDatabaseCommands
					.ForDatabase(dbname)
					.DeleteIndexAsync("Test");

				var verify_delete = documentStore.AsyncDatabaseCommands
					.ForDatabase(dbname)
					.GetIndexNamesAsync(0, 25);
				yield return verify_delete;

				//NOTE: this is failing because Silverlight is caching the response from the first verification
				Assert.IsFalse(verify_delete.Result.Contains("Test"));
			}
		}

		[Asynchronous]
		public IEnumerable<Task> CanGetASingleIndexByName()
		{
			var dbname = GenerateNewDatabaseName();
			using (var documentStore = new DocumentStore {Url = Url + Port}.Initialize())
			{
				yield return documentStore.AsyncDatabaseCommands.EnsureDatabaseExistsAsync(dbname);

				yield return documentStore.AsyncDatabaseCommands
					.ForDatabase(dbname)
					.PutIndexAsync("Test", new IndexDefinition
					                       	{
					                       		Map = "from doc in docs.Companies select new { doc.Name }"
					                       	}, true);

				var task = documentStore.AsyncDatabaseCommands.ForDatabase(dbname).GetIndexAsync("Test");
				yield return task;

				Assert.AreEqual("Test", task.Result.Name);
			}
		}
	}
}