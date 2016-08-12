using System;
using System.Collections.Generic;
using Raven.Abstractions.Data;
using Raven.Client.Document;
using Raven.Client.Extensions;
using Raven.Database.Extensions;
using Raven.Json.Linq;
using Raven.Tests.Core.Utils.Entities;
using System.IO;
using System.Linq;
using Xunit;

namespace Raven.Tests.Core.Commands
{
    public class Admin : RavenCoreTestBase
    {
        private string RestoreDir = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Restore.Data");
        private string BackupDir = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Backup");

        public Admin()
        {
            IOExtensions.DeleteFile(BackupDir);
            IOExtensions.DeleteFile(RestoreDir);

            IOExtensions.DeleteDirectory(BackupDir);
            IOExtensions.DeleteDirectory(RestoreDir);
        }

        public override void Dispose()
        {
            base.Dispose();

            IOExtensions.DeleteDirectory(BackupDir);
            IOExtensions.DeleteDirectory(RestoreDir);
        }

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
            using (var store = GetDocumentStore(modifyDatabaseDocument: doc => doc.Settings.Add(Constants.RunInMemory, "false")))
            {
                store.DatabaseCommands.Put("companies/1", null, RavenJObject.FromObject(new Company()), new RavenJObject());

                store.DatabaseCommands.GlobalAdmin.StartBackup(BackupDir, new DatabaseDocument()
                {
                    Settings = new Dictionary<string, string>()
                    {
                        { Constants.RunInMemory, "false" }
                    }
                }, false, store.DefaultDatabase);
                WaitForBackup(store.DatabaseCommands, true);
            }

            Server.DocumentStore.DatabaseCommands.GlobalAdmin.StartRestore(new DatabaseRestoreRequest()
            {
                BackupLocation = BackupDir,
                DatabaseLocation = RestoreDir,
                DatabaseName = "CanDoBackupAndRestore_Database"
            });

            WaitForRestore(Server.DocumentStore.DatabaseCommands);

            using (var store = new DocumentStore
            {
                Url = Server.SystemDatabase.ServerUrl,
                DefaultDatabase = "CanDoBackupAndRestore_Database",
            }.Initialize())
            {
                WaitForDocument(store.DatabaseCommands, "companies/1");
            }

            Server.DocumentStore.DatabaseCommands.GlobalAdmin.DeleteDatabase("CanDoBackupAndRestore_Database", hardDelete: true);
        }
    }
}
