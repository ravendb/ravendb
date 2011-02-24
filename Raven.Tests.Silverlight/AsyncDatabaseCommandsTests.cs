namespace Raven.Tests.Silverlight
{
	using System.Linq;
	using System.Collections.Generic;
	using System.Threading.Tasks;
	using Client.Document;
	using Client.Extensions;
	using Document;
	using Microsoft.Silverlight.Testing;
	using Microsoft.VisualStudio.TestTools.UnitTesting;

	public class AsyncDatabaseCommandsTests : RavenTestBase
	{
		[Asynchronous]
		public IEnumerable<Task> Can_get_documents_async()
		{
			var dbname = GenerateNewDatabaseName();
			var store = new DocumentStore { Url = Url + Port };
			store.Initialize();
			var cmd = store.AsyncDatabaseCommands;
			yield return cmd.EnsureDatabaseExistsAsync(dbname);

			using (var session = store.OpenAsyncSession(dbname))
			{
				session.Store(new Company { Name = "Hai" });
				session.Store(new Company { Name = "I can haz cheezburgr?" });
				session.Store(new Company { Name = "lol" });
				yield return session.SaveChangesAsync();
			}

			var task = cmd.ForDatabase(dbname).GetDocumentsAsync(0, 25);
			yield return task;

			Assert.AreEqual(3, task.Result.Length);
		}

		[Asynchronous]
		public IEnumerable<Task> Can_get_documents_whose_id_starts_with_a_prefix()
		{
			var dbname = GenerateNewDatabaseName();
			var store = new DocumentStore { Url = Url + Port };
			store.Initialize();
			var cmd = store.AsyncDatabaseCommands;
			yield return cmd.EnsureDatabaseExistsAsync(dbname);

			using (var session = store.OpenAsyncSession(dbname))
			{
				session.Store(new Company { Name = "Something with the desired prefix" });
				session.Store(new Contact { Surname = "Something without the desired prefix" });
				yield return session.SaveChangesAsync();
			}

			var task = cmd
				.ForDatabase(dbname)
				.GetDocumentsStartingWithAsync("Companies", 0, 25);
			yield return task;

			Assert.AreEqual(1, task.Result.Length);
		}

		[Asynchronous]
		public IEnumerable<Task> Can_get_a_list_of_databases_async()
		{
			var dbname = GenerateNewDatabaseName();
			var documentStore = new DocumentStore { Url = Url + Port };
			documentStore.Initialize();
			yield return documentStore.AsyncDatabaseCommands.EnsureDatabaseExistsAsync(dbname);

			var task = documentStore.AsyncDatabaseCommands.GetDatabaseNamesAsync();
			yield return task;

			Assert.IsTrue(task.Result.Contains(dbname));
		}

		[Asynchronous]
		public IEnumerable<Task> Should_not_cache_the_list_of_databases()
		{
			var documentStore = new DocumentStore { Url = Url + Port };
			documentStore.Initialize();

			var first = GenerateNewDatabaseName();
			yield return documentStore.AsyncDatabaseCommands
				.EnsureDatabaseExistsAsync(first);

			var task = documentStore.AsyncDatabaseCommands
				.GetDatabaseNamesAsync();
			yield return task;

			Assert.IsTrue(task.Result.Contains(first));

			var second = GenerateNewDatabaseName();
			yield return documentStore.AsyncDatabaseCommands
				.EnsureDatabaseExistsAsync(second);

			var verify = documentStore.AsyncDatabaseCommands
				.GetDatabaseNamesAsync();
			yield return verify;

			Assert.IsTrue(verify.Result.Contains(second));
		}

		[Asynchronous]
		public IEnumerable<Task> Can_get_delete_a_dcoument_by_id()
		{
			var dbname = GenerateNewDatabaseName();
			var store = new DocumentStore { Url = Url + Port };
			store.Initialize();
			yield return store.AsyncDatabaseCommands.EnsureDatabaseExistsAsync(dbname);

			var entity = new Company { Name = "Async Company #1" };
			using (var session = store.OpenAsyncSession(dbname))
			{
				session.Store(entity);
				yield return session.SaveChangesAsync();

				yield return session.Advanced.AsyncDatabaseCommands
					.DeleteDocumentAsync(entity.Id);
			}

			using (var for_verifying = store.OpenAsyncSession(dbname))
			{
				var verification = for_verifying.LoadAsync<Company>(entity.Id);
				yield return verification;

				Assert.IsNull(verification.Result);
			}
		}

		[Asynchronous]
		public IEnumerable<Task> The_response_for_getting_documents_should_not_be_cached()
		{
			var dbname = GenerateNewDatabaseName();
			var store = new DocumentStore { Url = Url + Port };
			store.Initialize();
			var cmd = store.AsyncDatabaseCommands;
			yield return cmd.EnsureDatabaseExistsAsync(dbname);

			using (var session = store.OpenAsyncSession(dbname))
			{
				session.Store(new Company { Name = "Hai" });
				session.Store(new Company { Name = "I can haz cheezburgr?" });
				session.Store(new Company { Name = "lol" });
				yield return session.SaveChangesAsync();
			}

			var task = cmd.ForDatabase(dbname).GetDocumentsAsync(0, 25);
			yield return task;

			Assert.AreEqual(3, task.Result.Length);

			using (var session = store.OpenAsyncSession(dbname))
			{
				yield return session.Advanced.AsyncDatabaseCommands
					.DeleteDocumentAsync(task.Result[0].Key);

				var second = cmd.ForDatabase(dbname).GetDocumentsAsync(0, 25);
				yield return second;

				Assert.AreEqual(2, second.Result.Length);
			}
		}
	}
}