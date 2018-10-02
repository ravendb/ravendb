// -----------------------------------------------------------------------
//  <copyright file="RavenDB_1716.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Database.Config;
using Raven.Json.Linq;
using Raven.Tests.Common;

using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.Issues
{
    public class RavenDB_2699 : RavenTest
    {
        protected override void ModifyConfiguration(InMemoryRavenConfiguration config)
        {
            config.Settings[Constants.Esent.CircularLog] = "false";
            config.Settings[Constants.Voron.AllowIncrementalBackups] = "true";
            config.Storage.Voron.AllowIncrementalBackups = true;
        }

        [Theory(Skip = "Flaky test")]
        [PropertyData("Storages")]
        public async Task Restore_incremental_backup_works_propertly(string storage)
        {
            var backupDir = NewDataPath("BackupDatabase");
            var restoreDir = NewDataPath("RestoredDatabase");

            using (var store = NewRemoteDocumentStore(runInMemory: false, requestedStorage: storage))
            {
                for (int i = 1; i <= 500; i++)
                {
                    store.DatabaseCommands.Put("keys/" + i, null, new RavenJObject { { "Key", 1 } }, new RavenJObject());
                }

                var operation = await store.AsyncDatabaseCommands.GlobalAdmin.StartBackupAsync(backupDir, null, true, store.DefaultDatabase);
                operation.WaitForCompletion();

                // do an empty backup
                operation = await store.AsyncDatabaseCommands.GlobalAdmin.StartBackupAsync(backupDir, null, true, store.DefaultDatabase);
                operation.WaitForCompletion();

                // put more data
                for (int i = 501; i <= 1000; i++)
                {
                    store.DatabaseCommands.Put("keys/" + i, null, new RavenJObject { { "Key", 2 } }, new RavenJObject());
                }

                operation = await store.AsyncDatabaseCommands.GlobalAdmin.StartBackupAsync(backupDir, null, true, store.DefaultDatabase);
                operation.WaitForCompletion();

                // restore as a new database
                await store.AsyncDatabaseCommands.GlobalAdmin.StartRestoreAsync(new DatabaseRestoreRequest
                {
                    BackupLocation = backupDir,
                    DatabaseLocation = restoreDir,
                    DatabaseName = "db1"
                });

                // get restore status and wait for finish
                WaitForRestore(store.DatabaseCommands);
                WaitForDocument(store.DatabaseCommands.ForSystemDatabase(), "Raven/Databases/db1");

                Assert.Equal(1, store.DatabaseCommands.ForDatabase("db1").Get("keys/1").DataAsJson.Value<int>("Key"));
                Assert.Equal(2, store.DatabaseCommands.ForDatabase("db1").Get("keys/1000").DataAsJson.Value<int>("Key"));
            }
        }
    }
}
