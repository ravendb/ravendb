// -----------------------------------------------------------------------
//  <copyright file="RavenDB_1716.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Json.Linq;
using Raven.Tests.Common;

using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.Issues
{
    public class RavenDB_1716 : RavenTest
    {
        [Theory]
        [PropertyData("Storages")]
        public async Task Restore_operation_works_async(string storage)
        {
            var backupDir = NewDataPath("BackupDatabase");
            var restoreDir = NewDataPath("RestoredDatabase");

            using (var store = NewRemoteDocumentStore(runInMemory: false, requestedStorage: storage, databaseName: Constants.SystemDatabase))
            {
                store.DatabaseCommands.Put("keys/1", null, new RavenJObject { { "Key", 1 } }, new RavenJObject());

                await store.AsyncDatabaseCommands.GlobalAdmin.StartBackupAsync(backupDir, new DatabaseDocument(), false, Constants.SystemDatabase);

                WaitForBackup(store.DatabaseCommands, true);

                // restore as a new database
                await store.AsyncDatabaseCommands.GlobalAdmin.StartRestoreAsync(new RestoreRequest
                {
                    BackupLocation = backupDir,
                    DatabaseLocation = restoreDir,
                    DatabaseName = "db1"
                });

                // get restore status and wait for finish
                WaitForRestore(store.DatabaseCommands);
                WaitForDocument(store.DatabaseCommands, "Raven/Databases/db1");

                Assert.Equal(1, store.DatabaseCommands.ForDatabase("db1").Get("keys/1").DataAsJson.Value<int>("Key"));
            }
        }
    }
}