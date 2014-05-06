using Raven.Client.Connection;
using Raven.Client.Connection.Async;
using Raven.Client.Document;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
	public class BulkInsertDatabaseUrl : RavenTest
	{
		[Fact]
		public void ShouldHaveTheCorrectDatabaseName_WhenTheDatabaseIsSpecifiedInTheUrlOnly()
		{
			using (var server = GetNewServer())
			{
				// Create the database, otherwise bulkInsert will throw because of 404 response
				using (var store = new DocumentStore {Url = "http://localhost:8079", DefaultDatabase = "my-db-name"}.Initialize())
				{
				}

				using (var store = new DocumentStore {Url = "http://localhost:8079/databases/my-db-name"}.Initialize())
				using (var bulkInsert = store.BulkInsert())
				{
					Assert.Equal("http://localhost:8079/databases/my-db-name", ((AsyncServerClient) bulkInsert.DatabaseCommands).Url);
				}
			}
		}
	}
}