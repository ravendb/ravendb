using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Silverlight.Testing;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Raven.Client.Document;
using Raven.Client.Document.Async;
using Raven.Client.Extensions;
using Raven.Tests.Document;

namespace Raven.Tests.Silverlight
{
	public class AsyncDatabaseCommandsTests : RavenTestBase
	{
		[Asynchronous]
		public IEnumerable<Task> CanGetDocumentsAsync()
		{
			var dbname = GenerateNewDatabaseName();
			using (var documentStore = new DocumentStore { Url = Url + Port }.Initialize())
			{
				var cmd = documentStore.AsyncDatabaseCommands;
				yield return cmd.EnsureDatabaseExistsAsync(dbname);

				using (var session = documentStore.OpenAsyncSession(dbname))
				{
					session.Store(new Company {Name = "Hai"});
					session.Store(new Company {Name = "I can haz cheezburgr?"});
					session.Store(new Company {Name = "lol"});
					yield return session.SaveChangesAsync();
				}

				var task = cmd.ForDatabase(dbname).GetDocumentsAsync(0, 25);
				yield return task;

				Assert.AreEqual(3, task.Result.Length);
			}
		}

		[Asynchronous]
		public IEnumerable<Task> CanGetDocumentsWhoseIdStartsWithAPrefix()
		{
			var dbname = GenerateNewDatabaseName();
			using (var documentStore = new DocumentStore {Url = Url + Port}.Initialize())
			{
				var cmd = documentStore.AsyncDatabaseCommands;
				yield return cmd.EnsureDatabaseExistsAsync(dbname);

				using (var session = documentStore.OpenAsyncSession(dbname))
				{
					session.Store(new Company {Name = "Something with the desired prefix"});
					session.Store(new Contact {Surname = "Something without the desired prefix"});
					yield return session.SaveChangesAsync();
				}

				var task = cmd
					.ForDatabase(dbname)
					.GetDocumentsStartingWithAsync("Companies", 0, 25);
				yield return task;

				Assert.AreEqual(1, task.Result.Length);
			}
		}

		[Asynchronous]
		public IEnumerable<Task> CanGetAListOfDatabasesAsync()
		{
			var dbname = GenerateNewDatabaseName();
			using (var documentStore = new DocumentStore {Url = Url + Port}.Initialize())
			{
				yield return documentStore.AsyncDatabaseCommands.EnsureDatabaseExistsAsync(dbname);

				var task = documentStore.AsyncDatabaseCommands.GetDatabaseNamesAsync(25);
				yield return task;

				Assert.IsTrue(task.Result.Contains(dbname));
			}
		}

		[Asynchronous]
		public IEnumerable<Task> ShouldNotCacheTheListOfDatabases()
		{
			var first = GenerateNewDatabaseName();
			using (var documentStore = new DocumentStore {Url = Url + Port}.Initialize())
			{
				yield return documentStore.AsyncDatabaseCommands
					.EnsureDatabaseExistsAsync(first);

				var task = documentStore.AsyncDatabaseCommands
					.GetDatabaseNamesAsync(25);
				yield return task;

				Assert.IsTrue(task.Result.Contains(first));

				var second = GenerateNewDatabaseName();
				yield return documentStore.AsyncDatabaseCommands
					.EnsureDatabaseExistsAsync(second);

				var verify = documentStore.AsyncDatabaseCommands
					.GetDatabaseNamesAsync(25);
				yield return verify;

				Assert.IsTrue(verify.Result.Contains(second));
			}
		}

		[Asynchronous]
		public IEnumerable<Task> CanGetDeleteADcoumentById()
		{
			var dbname = GenerateNewDatabaseName();
			using (var documentStore = new DocumentStore {Url = Url + Port}.Initialize())
			{
				yield return documentStore.AsyncDatabaseCommands.EnsureDatabaseExistsAsync(dbname);

				var entity = new Company {Name = "Async Company #1"};
				using (var session = documentStore.OpenAsyncSession(dbname))
				{
					session.Store(entity);
					yield return session.SaveChangesAsync();

					yield return ((AsyncDocumentSession)session).AsyncDatabaseCommands
						.DeleteDocumentAsync(entity.Id);
				}

				using (var for_verifying = documentStore.OpenAsyncSession(dbname))
				{
					var verification = for_verifying.LoadAsync<Company>(entity.Id);
					yield return verification;

					Assert.IsNull(verification.Result);
				}
			}
		}

		[Asynchronous]
		public IEnumerable<Task> TheResponseForGettingDocumentsShouldNotBeCached()
		{
			var dbname = GenerateNewDatabaseName();
			using (var documentStore = new DocumentStore {Url = Url + Port}.Initialize())
			{
				var cmd = documentStore.AsyncDatabaseCommands;
				yield return cmd.EnsureDatabaseExistsAsync(dbname);

				using (var session = documentStore.OpenAsyncSession(dbname))
				{
					session.Store(new Company {Name = "Hai"});
					session.Store(new Company {Name = "I can haz cheezburgr?"});
					session.Store(new Company {Name = "lol"});
					yield return session.SaveChangesAsync();
				}

				var task = cmd.ForDatabase(dbname).GetDocumentsAsync(0, 25);
				yield return task;

				Assert.AreEqual(3, task.Result.Length);

				using (var session = documentStore.OpenAsyncSession(dbname))
				{
					yield return ((AsyncDocumentSession)session).AsyncDatabaseCommands
						.DeleteDocumentAsync(task.Result[0].Key);

					var second = cmd.ForDatabase(dbname).GetDocumentsAsync(0, 25);
					yield return second;

					Assert.AreEqual(2, second.Result.Length);
				}
			}
		}
	}
}