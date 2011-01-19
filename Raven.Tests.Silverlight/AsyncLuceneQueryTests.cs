namespace Raven.Tests.Silverlight
{
	using System.Collections.Generic;
	using System.Threading.Tasks;
	using Client.Document;
	using Client.Extensions;
	using Microsoft.Silverlight.Testing;
	using Xunit;

	public class AsyncLuceneQueryTests : RavenTestBase
	{
		[Asynchronous]
		public IEnumerable<Task> Can_query_using_async_session()
		{
			var dbname = GenerateNewDatabaseName();
			var documentStore = new DocumentStore {Url = Url + Port};
			documentStore.Initialize();
			yield return documentStore.AsyncDatabaseCommands.EnsureDatabaseExistsAsync(dbname);

			using (var s = documentStore.OpenAsyncSession(dbname))
			{
				s.Store(new {Name = "Ayende"});
				yield return s.SaveChangesAsync();
			}

			using (var s = documentStore.OpenAsyncSession(dbname))
			{
				var queryResultAsync = s.Advanced.AsyncLuceneQuery<object>()
					.WhereEquals("Name", "Ayende")
					.QueryResultAsync;

				yield return queryResultAsync;

				Assert.Equal("Ayende", queryResultAsync.Result.Results[0].Value<string>("Name"));
			}
		}
	}
}