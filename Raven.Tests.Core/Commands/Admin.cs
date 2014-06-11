using Raven.Client.Extensions;
using System.Linq;
using Xunit;

namespace Raven.Tests.Core.Commands
{
	public class Admin : RavenCoreTestBase
	{
		[Fact]
		public void CanManageIndexingProcess()
		{
			using (var store = GetDocumentStore())
			{
				var adminCommands = store.DatabaseCommands.Admin;

				var indexingStatus = adminCommands.GetIndexingStatus();

				Assert.Equal("Indexing", indexingStatus);

				adminCommands.StopIndexing();

				indexingStatus = adminCommands.GetIndexingStatus();

				Assert.Equal("Paused", indexingStatus);

				adminCommands.StartIndexing();

				indexingStatus = adminCommands.GetIndexingStatus();

				Assert.Equal("Indexing", indexingStatus);
			}
		}

        [Fact]
        public void CanCreateAndDeleteDatabase()
        {
            using (var store = GetDocumentStore())
            {
                var globalAdmin = store.DatabaseCommands.GlobalAdmin;

                const string databaseName = "SampleDb_Of_CanCreateAndDeleteDatabase_Test";

                var databaseDocument = MultiDatabase.CreateDatabaseDocument(databaseName);

                globalAdmin.CreateDatabase(databaseDocument);

                Assert.NotNull(store.DatabaseCommands.ForSystemDatabase().Get(databaseDocument.Id));

                globalAdmin.DeleteDatabase(databaseName, true);

                Assert.Null(store.DatabaseCommands.ForSystemDatabase().Get(databaseDocument.Id));

                globalAdmin.EnsureDatabaseExists(databaseName);

                Assert.NotNull(store.DatabaseCommands.ForSystemDatabase().Get(databaseDocument.Id));

                globalAdmin.DeleteDatabase(databaseName, true);

                Assert.Null(store.DatabaseCommands.ForSystemDatabase().Get(databaseDocument.Id));
            }
        }

        [Fact]
        public void CanGetServerStatistics()
        {
            using (var store = GetDocumentStore())
            {
                var globalAdmin = store.DatabaseCommands.GlobalAdmin;

                var adminStatistics = globalAdmin.GetStatistics();

                Assert.NotNull(adminStatistics);

                Assert.Equal(TestServerFixture.ServerName, adminStatistics.ServerName);
                Assert.True(adminStatistics.LoadedDatabases.Any());
                Assert.True(adminStatistics.TotalNumberOfRequests > 0);
                Assert.NotNull(adminStatistics.Memory);
            }
        }
	}
}