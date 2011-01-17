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
		public IEnumerable<Task> Can_query_by_index()
		{
			var dbname = GenerateNewDatabaseName();
			var documentStore = new DocumentStore {Url = Url + Port};
			documentStore.Initialize();
			yield return documentStore.AsyncDatabaseCommands.EnsureDatabaseExistsAsync(dbname);

			var entity = new Company {Name = "Async Company #1", Id = "companies/1"};
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

				Assert.NotEqual(0, query.Result.Count);
			}
		}
	}
}