using System;
using System.IO;
using FastTests;
using Orders;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Database;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Utils;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_16367 : RavenTestBase
    {
        public RavenDB_16367(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanLockDatabase()
        {
            using (var store = GetDocumentStore())
            {
                var databaseName1 = $"{store.Database}_LockMode_1";
                var databaseName2 = $"{store.Database}_LockMode_2";
                var databaseName3 = $"{store.Database}_LockMode_3";

                Assert.Throws<DatabaseDoesNotExistException>(() => store.Maintenance.Server.Send(new SetDatabasesLockOperation(databaseName1, DatabaseLockMode.PreventDeletesError)));

                store.Maintenance.Server.Send(new CreateDatabaseOperation(new DatabaseRecord(databaseName1)));

                var databaseRecord = store.Maintenance.Server.Send(new GetDatabaseRecordOperation(databaseName1));
                Assert.Equal(DatabaseLockMode.Unlock, databaseRecord.LockMode);

                store.Maintenance.Server.Send(new SetDatabasesLockOperation(databaseName1, DatabaseLockMode.Unlock));

                databaseRecord = store.Maintenance.Server.Send(new GetDatabaseRecordOperation(databaseName1));
                Assert.Equal(DatabaseLockMode.Unlock, databaseRecord.LockMode);

                store.Maintenance.Server.Send(new SetDatabasesLockOperation(databaseName1, DatabaseLockMode.PreventDeletesError));

                databaseRecord = store.Maintenance.Server.Send(new GetDatabaseRecordOperation(databaseName1));
                Assert.Equal(DatabaseLockMode.PreventDeletesError, databaseRecord.LockMode);

                var e = Assert.Throws<RavenException>(() => store.Maintenance.Server.Send(new DeleteDatabasesOperation(databaseName1, hardDelete: true)));
                Assert.Contains("cannot be deleted because of the set lock mode ('PreventDeletesError')", e.Message);

                store.Maintenance.Server.Send(new SetDatabasesLockOperation(databaseName1, DatabaseLockMode.PreventDeletesIgnore));

                databaseRecord = store.Maintenance.Server.Send(new GetDatabaseRecordOperation(databaseName1));
                Assert.Equal(DatabaseLockMode.PreventDeletesIgnore, databaseRecord.LockMode);

                var result = store.Maintenance.Server.Send(new DeleteDatabasesOperation(databaseName1, hardDelete: true));
                Assert.Equal(-1, result.RaftCommandIndex);
                Assert.True(result.PendingDeletes == null || result.PendingDeletes.Length == 0);

                databaseRecord = store.Maintenance.Server.Send(new GetDatabaseRecordOperation(databaseName1));
                Assert.NotNull(databaseRecord);
                Assert.True(databaseRecord.DeletionInProgress == null || databaseRecord.DeletionInProgress.Count == 0);

                store.Maintenance.Server.Send(new CreateDatabaseOperation(new DatabaseRecord(databaseName2)));

                result = store.Maintenance.Server.Send(new DeleteDatabasesOperation(new DeleteDatabasesOperation.Parameters
                {
                    DatabaseNames = new[] { databaseName1, databaseName2 },
                    HardDelete = true
                }));

                Assert.True(result.RaftCommandIndex > 0);

                databaseRecord = store.Maintenance.Server.Send(new GetDatabaseRecordOperation(databaseName1));
                Assert.NotNull(databaseRecord);
                Assert.True(databaseRecord.DeletionInProgress == null || databaseRecord.DeletionInProgress.Count == 0);

                databaseRecord = store.Maintenance.Server.Send(new GetDatabaseRecordOperation(databaseName2));
                if (databaseRecord != null)
                    Assert.True(databaseRecord.DeletionInProgress != null && databaseRecord.DeletionInProgress.Count > 0);

                store.Maintenance.Server.Send(new SetDatabasesLockOperation(databaseName1, DatabaseLockMode.Unlock));

                result = store.Maintenance.Server.Send(new DeleteDatabasesOperation(databaseName1, hardDelete: true));
                Assert.True(result.RaftCommandIndex > 0);

                databaseRecord = store.Maintenance.Server.Send(new GetDatabaseRecordOperation(databaseName1));
                if (databaseRecord != null)
                    Assert.True(databaseRecord.DeletionInProgress != null && databaseRecord.DeletionInProgress.Count > 0);

                store.Maintenance.Server.Send(new CreateDatabaseOperation(new DatabaseRecord(databaseName3) { LockMode = DatabaseLockMode.PreventDeletesIgnore }));

                databaseRecord = store.Maintenance.Server.Send(new GetDatabaseRecordOperation(databaseName3));
                Assert.Equal(DatabaseLockMode.PreventDeletesIgnore, databaseRecord.LockMode);

                store.Maintenance.Server.Send(new SetDatabasesLockOperation(databaseName3, DatabaseLockMode.Unlock));

                result = store.Maintenance.Server.Send(new DeleteDatabasesOperation(databaseName3, hardDelete: true));
                Assert.True(result.RaftCommandIndex > 0);

                databaseRecord = store.Maintenance.Server.Send(new GetDatabaseRecordOperation(databaseName3));
                if (databaseRecord != null)
                    Assert.True(databaseRecord.DeletionInProgress != null && databaseRecord.DeletionInProgress.Count > 0);
            }
        }

        [Fact]
        public void CanLockDatabase_Multiple()
        {
            using (var store = GetDocumentStore())
            {
                var databaseName1 = $"{store.Database}_LockMode_1";
                var databaseName2 = $"{store.Database}_LockMode_2";
                var databaseName3 = $"{store.Database}_LockMode_3";

                var databases = new[] { databaseName1, databaseName2, databaseName3 };

                Assert.Throws<DatabaseDoesNotExistException>(() => store.Maintenance.Server.Send(new SetDatabasesLockOperation(new SetDatabasesLockOperation.Parameters { DatabaseNames = databases, Mode = DatabaseLockMode.PreventDeletesError })));

                store.Maintenance.Server.Send(new CreateDatabaseOperation(new DatabaseRecord(databaseName1)));

                Assert.Throws<DatabaseDoesNotExistException>(() => store.Maintenance.Server.Send(new SetDatabasesLockOperation(new SetDatabasesLockOperation.Parameters { DatabaseNames = databases, Mode = DatabaseLockMode.PreventDeletesError })));

                AssertLockMode(store, databaseName1, DatabaseLockMode.Unlock);

                store.Maintenance.Server.Send(new CreateDatabaseOperation(new DatabaseRecord(databaseName2)));
                store.Maintenance.Server.Send(new CreateDatabaseOperation(new DatabaseRecord(databaseName3)));

                AssertLockMode(store, databaseName2, DatabaseLockMode.Unlock);
                AssertLockMode(store, databaseName3, DatabaseLockMode.Unlock);

                store.Maintenance.Server.Send(new SetDatabasesLockOperation(new SetDatabasesLockOperation.Parameters { DatabaseNames = databases, Mode = DatabaseLockMode.PreventDeletesError }));

                AssertLockMode(store, databaseName1, DatabaseLockMode.PreventDeletesError);
                AssertLockMode(store, databaseName2, DatabaseLockMode.PreventDeletesError);
                AssertLockMode(store, databaseName3, DatabaseLockMode.PreventDeletesError);

                store.Maintenance.Server.Send(new SetDatabasesLockOperation(databaseName2, DatabaseLockMode.PreventDeletesIgnore));

                AssertLockMode(store, databaseName1, DatabaseLockMode.PreventDeletesError);
                AssertLockMode(store, databaseName2, DatabaseLockMode.PreventDeletesIgnore);
                AssertLockMode(store, databaseName3, DatabaseLockMode.PreventDeletesError);

                store.Maintenance.Server.Send(new SetDatabasesLockOperation(new SetDatabasesLockOperation.Parameters { DatabaseNames = databases, Mode = DatabaseLockMode.PreventDeletesIgnore }));

                AssertLockMode(store, databaseName1, DatabaseLockMode.PreventDeletesIgnore);
                AssertLockMode(store, databaseName2, DatabaseLockMode.PreventDeletesIgnore);
                AssertLockMode(store, databaseName3, DatabaseLockMode.PreventDeletesIgnore);

                store.Maintenance.Server.Send(new SetDatabasesLockOperation(new SetDatabasesLockOperation.Parameters { DatabaseNames = databases, Mode = DatabaseLockMode.Unlock }));

                store.Maintenance.Server.Send(new DeleteDatabasesOperation(databaseName1, hardDelete: true));
                store.Maintenance.Server.Send(new DeleteDatabasesOperation(databaseName2, hardDelete: true));
                store.Maintenance.Server.Send(new DeleteDatabasesOperation(databaseName3, hardDelete: true));
            }
        }

        [Fact]
        public void CanLockDatabase_Disabled()
        {
            using (var store = GetDocumentStore())
            {
                var databaseName = $"{store.Database}_LockMode_1";

                store.Maintenance.Server.Send(new CreateDatabaseOperation(new DatabaseRecord(databaseName)));

                AssertLockMode(store, databaseName, DatabaseLockMode.Unlock);

                store.Maintenance.Server.Send(new ToggleDatabasesStateOperation(databaseName, disable: true));

                store.Maintenance.Server.Send(new SetDatabasesLockOperation(databaseName, DatabaseLockMode.PreventDeletesError));

                AssertLockMode(store, databaseName, DatabaseLockMode.PreventDeletesError);

                store.Maintenance.Server.Send(new SetDatabasesLockOperation(databaseName, DatabaseLockMode.Unlock));

                store.Maintenance.Server.Send(new DeleteDatabasesOperation(databaseName, hardDelete: true));
            }
        }

        [Fact]
        public void CanLockDatabase_Backup_Restore()
        {
            var backupPath = NewDataPath();
            IOExtensions.DeleteDirectory(backupPath);

            using (var store = GetDocumentStore())
            {
                var databaseName1 = $"{store.Database}_LockMode_1";
                var databaseName2 = $"{store.Database}_LockMode_2";

                store.Maintenance.Server.Send(new CreateDatabaseOperation(new DatabaseRecord(databaseName1) { LockMode = DatabaseLockMode.PreventDeletesError }));

                var databaseRecord = store.Maintenance.Server.Send(new GetDatabaseRecordOperation(databaseName1));
                Assert.Equal(DatabaseLockMode.PreventDeletesError, databaseRecord.LockMode);

                using (var session = store.OpenSession(databaseName1))
                {
                    session.Store(new Company { Name = "HR" });
                    session.SaveChanges();
                }

                var operation = store.Maintenance.ForDatabase(databaseName1).Send(new BackupOperation(new BackupConfiguration
                {
                    LocalSettings = new LocalSettings
                    {
                        FolderPath = backupPath
                    }
                }));

                var result = (BackupResult)operation.WaitForCompletion(TimeSpan.FromSeconds(30));

                store.Maintenance.Server.Send(new SetDatabasesLockOperation(databaseName1, DatabaseLockMode.Unlock));
                store.Maintenance.Server.Send(new DeleteDatabasesOperation(databaseName1, hardDelete: true));

                using (Backup.RestoreDatabase(store, new RestoreBackupConfiguration
                {
                    DatabaseName = databaseName2,
                    BackupLocation = Path.Combine(backupPath, result.LocalBackup.BackupDirectory)
                }, TimeSpan.FromSeconds(30)))
                {
                    databaseRecord = store.Maintenance.Server.Send(new GetDatabaseRecordOperation(databaseName2));
                    Assert.Equal(DatabaseLockMode.PreventDeletesError, databaseRecord.LockMode);

                    var e = Assert.Throws<RavenException>(() => store.Maintenance.Server.Send(new DeleteDatabasesOperation(databaseName2, hardDelete: true)));
                    Assert.Contains("cannot be deleted because of the set lock mode ('PreventDeletesError')", e.Message);

                    store.Maintenance.Server.Send(new SetDatabasesLockOperation(databaseName2, DatabaseLockMode.Unlock));
                }
            }
        }

        private static void AssertLockMode(IDocumentStore store, string databaseName, DatabaseLockMode mode)
        {
            var databaseRecord = store.Maintenance.Server.Send(new GetDatabaseRecordOperation(databaseName));
            Assert.Equal(mode, databaseRecord.LockMode);
        }
    }
}
