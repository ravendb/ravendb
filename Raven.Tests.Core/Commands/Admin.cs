using Raven.Abstractions.Data;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Client.Extensions;
using Raven.Database.Extensions;
using Raven.Json.Linq;
using Raven.Tests.Core.Utils.Entities;
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
				Assert.NotNull(adminStatistics.LoadedDatabases.First().StorageStats);
            }
        }

        [Fact]
        public void CanDoBackupAndRestore()
        {
            const string DataDir = "C:\\raventest";
            const string BackupDir = "C:\\";

            using (var store = GetDocumentStore(modifyDatabaseDocument: doc => doc.Settings.Add("DataDirectory", DataDir)))
            {
                store.DatabaseCommands.Put("companies/1", null, RavenJObject.FromObject(new Company()), new RavenJObject());
                store.DatabaseCommands.GlobalAdmin.StartBackup("C:\\", new DatabaseDocument(), false, store.DefaultDatabase);
                WaitForBackup(Server.SystemDatabase);

                store.Dispose();
                base.Dispose();
                IOExtensions.DeleteDirectory(DataDir);
            }

            Server.DocumentStore.DatabaseCommands.GlobalAdmin.StartRestore(new RestoreRequest { BackupLocation = BackupDir, DatabaseLocation = DataDir, DatabaseName = "CanDoBackupAndRestore" });
            WaitForRestore(Server.DocumentStore.DatabaseCommands);

            using (var store = new DocumentStore
            {
                Url = Server.SystemDatabase.ServerUrl,
                DefaultDatabase = "CanDoBackupAndRestore"
            }.Initialize())
            {
                WaitForDocument(store.DatabaseCommands, "companies/1");
            }
        }
	}
}