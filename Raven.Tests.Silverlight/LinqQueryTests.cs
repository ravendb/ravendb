namespace Raven.Tests.Silverlight
{
	using System.Collections.Generic;
	using System.Linq;
	using System.Threading.Tasks;
	using Client.Document;
	using Client.Extensions;
	using Client.Linq;
	using Database.Data;
	using Database.Indexing;
	using Document;
	using Microsoft.Silverlight.Testing;
	using Xunit;

	public class LinqQueryTests : RavenTestBase
	{
		[Asynchronous]
		public IEnumerable<Task> Can_perform_a_simple_linq_query_asychronously()
		{
			var dbname = GenerateNewDatabaseName();
			var documentStore = new DocumentStore { Url = Url + Port };
			documentStore.Initialize();
			yield return documentStore.AsyncDatabaseCommands.EnsureDatabaseExistsAsync(dbname);

			var entity = new Company { Name = "Async Company #1", Id = "companies/1" };
			using (var session = documentStore.OpenAsyncSession(dbname))
			{
				session.Store(entity);
				yield return session.SaveChangesAsync();
			}

			using (var session = documentStore.OpenAsyncSession(dbname))
			{
				var query = session.Query<Company>()
							.Where(x => x.Name == "Async Company #1")
							.ToListAsync();
				yield return query;

				Assert.Equal(1, query.Result.Count);
				Assert.Equal("Async Company #1", query.Result[0].Name);
			}
		}

		[Asynchronous]
		public IEnumerable<Task> Can_perform_a_projection_in_a_linq_query()
		{
			var dbname = GenerateNewDatabaseName();
			var documentStore = new DocumentStore { Url = Url + Port };
			documentStore.Initialize();
			yield return documentStore.AsyncDatabaseCommands.EnsureDatabaseExistsAsync(dbname);

			var entity = new Company { Name = "Async Company #1", Id = "companies/1" };
			using (var session = documentStore.OpenAsyncSession(dbname))
			{
				session.Store(entity);
				yield return session.SaveChangesAsync();
			}

			using (var session = documentStore.OpenAsyncSession(dbname))
			{
				var query = session.Query<Company>()
							.Where(x => x.Name == "Async Company #1")
							.Select(x => x.Name)
							.ToListAsync();
				yield return query;

				Assert.Equal(1, query.Result.Count);
				Assert.Equal("Async Company #1", query.Result[0]);
			}
		}
	}
}