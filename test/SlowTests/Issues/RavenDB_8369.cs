using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Backups;
using Raven.Server.ServerWide.Context;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_8369 : ReplicationTestBase
    {
        public class FooBar
        {
            public string Foo { get; set; }
        }

        [Fact]
        public async Task Tombstones_should_be_cleaned_only_after_backup_with_binary_backupwhen_delete_is_last_operation()
        {
            await Tombstones_should_be_cleaned_only_after_backup_when_delete_is_last_operation(BackupType.Backup, "My backup");
        }

        [Fact]
        public async Task Tombstones_should_be_cleaned_only_after_backup_with_snapshot_backupwhen_delete_is_last_operation()
        {
            await Tombstones_should_be_cleaned_only_after_backup_when_delete_is_last_operation(BackupType.Snapshot, "My backup");
        }

        [Fact]
        public async Task Tombstones_should_be_cleaned_only_after_backup_with_binary_backupwhen_delete_is_not_last_operation()
        {
            await Tombstones_should_be_cleaned_only_after_backup_when_delete_is_not_a_last_operation(BackupType.Backup, "My backup");
        }

        [Fact]
        public async Task Tombstones_should_be_cleaned_only_after_backup_with_snapshot_backupwhen_delete_is_not_last_operation()
        {
            await Tombstones_should_be_cleaned_only_after_backup_when_delete_is_not_a_last_operation(BackupType.Snapshot, "My backup");
        }

        [Fact]
        public async Task Tomstones_should_be_cleaned_properly_for_multiple_backup_tasks_for_snapshot()
        {
            await Tomstones_should_be_cleaned_properly_for_multiple_backup_tasks(BackupType.Snapshot);
        }

        [Fact]
        public async Task Tomstones_should_be_cleaned_properly_for_multiple_backup_tasks_for_backup()
        {
            await Tomstones_should_be_cleaned_properly_for_multiple_backup_tasks(BackupType.Backup);
        }

        public async Task Tomstones_should_be_cleaned_properly_for_multiple_backup_tasks(BackupType backupType)
        {
            var backupPath1 = NewDataPath(suffix: "BackupFolder");
            var backupPath2 = NewDataPath(suffix: "BackupFolder");
            using (var store = GetDocumentStore())
            {
                var backupOperationResult1 = await SetupBackupAsync(backupPath1, store, backupType, "Backup 1");
                var backupOperationResult2 = await SetupBackupAsync(backupPath2, store, backupType, "Backup 2");

                using (var session = store.OpenSession())
                {
                    session.Store(new FooBar
                    {
                        Foo = "Bar1"
                    }, "foo/bar1");
                    session.Store(new FooBar
                    {
                        Foo = "Bar2"
                    }, "foo/bar2");
                    session.SaveChanges();
                }

                var documentDatabase = (await GetDocumentDatabaseInstanceFor(store));

                using (var session = store.OpenSession())
                {
                    session.Delete("foo/bar1");
                    session.SaveChanges();
                }

                RunBackup(backupOperationResult1.TaskId, documentDatabase);
                RunBackup(backupOperationResult2.TaskId, documentDatabase);

                //force tombstone cleanup - now, after backup, tombstones should be cleaned
                await documentDatabase.TombstoneCleaner.ExecuteCleanup();

                using (documentDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var tombstonesCount = documentDatabase.DocumentsStorage.GetTombstonesFrom(context, 0, 0, int.MaxValue).Count();
                    Assert.Equal(0, tombstonesCount);
                }

                //now delete one more document, but execute backup only for ONE of backup tasks.
                //because the tombstone is not backed up by both tasks, only ONE will get deleted.
                using (var session = store.OpenSession())
                {
                    session.Delete("foo/bar2");
                    session.SaveChanges();
                }

                RunBackup(backupOperationResult1.TaskId, documentDatabase);
                await documentDatabase.TombstoneCleaner.ExecuteCleanup();

                //since we ran only one of backup tasks, only tombstones with minimal last etag get cleaned
                using (documentDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var tombstonesCount = documentDatabase.DocumentsStorage.GetTombstonesFrom(context, 0, 0, int.MaxValue).Count();
                    Assert.Equal(1, tombstonesCount);
                }
            }
        }

        public async Task Tombstones_should_be_cleaned_only_after_backup_when_delete_is_last_operation(BackupType backupType, string taskName)
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");
            using (var store = GetDocumentStore())
            {
                var result = await SetupBackupAsync(backupPath, store, backupType, taskName);

                using (var session = store.OpenSession())
                {
                    session.Store(new FooBar { Foo = "Bar1" }, "foo/bar1");
                    session.Store(new FooBar { Foo = "Bar2" }, "foo/bar2");
                    session.SaveChanges();
                }

                var documentDatabase = (await GetDocumentDatabaseInstanceFor(store));

                using (var session = store.OpenSession())
                {
                    session.Delete("foo/bar1");
                    session.Delete("foo/bar2");
                    session.SaveChanges();
                }

                await documentDatabase.TombstoneCleaner.ExecuteCleanup();

                using (documentDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var tombstones = documentDatabase.DocumentsStorage.GetTombstonesFrom(context, 0, 0, int.MaxValue).ToList();

                    //since we didn't backup the tombstones yet, they will not get cleaned
                    Assert.Equal(2, tombstones.Count);
                }

                RunBackup(result.TaskId, documentDatabase);

                //force tombstone cleanup - now, after backup, tombstones should be cleaned
                await documentDatabase.TombstoneCleaner.ExecuteCleanup();

                using (documentDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var tombstonesCount = documentDatabase.DocumentsStorage.GetTombstonesFrom(context, 0, 0, int.MaxValue).Count();
                    Assert.Equal(0, tombstonesCount);
                }
            }
        }

        public async Task Tombstones_should_be_cleaned_only_after_backup_when_delete_is_not_a_last_operation(BackupType backupType, string taskName)
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");
            using (var store = GetDocumentStore())
            {
                var result = await SetupBackupAsync(backupPath, store, backupType, taskName);

                using (var session = store.OpenSession())
                {
                    session.Store(new FooBar { Foo = "Bar1" }, "foo/bar1");
                    session.Store(new FooBar { Foo = "Bar2" }, "foo/bar2");
                    session.SaveChanges();
                }

                var documentDatabase = (await GetDocumentDatabaseInstanceFor(store));

                using (var session = store.OpenSession())
                {
                    session.Delete("foo/bar1");
                    session.Delete("foo/bar2");
                    session.SaveChanges();
                }

                await documentDatabase.TombstoneCleaner.ExecuteCleanup();

                using (documentDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var tombstones = documentDatabase.DocumentsStorage.GetTombstonesFrom(context, 0, 0, int.MaxValue).ToList();

                    //since we didn't backup the tombstones yet, they will not get cleaned
                    Assert.Equal(2, tombstones.Count);
                }

                //do a document PUT, so latest etag won't belong to tombstone
                using (var session = store.OpenSession())
                {
                    session.Store(new FooBar { Foo = "Bar3" }, "foo/bar3");
                    session.SaveChanges();
                }

                RunBackup(result.TaskId, documentDatabase);

                //force tombstone cleanup - now, after backup, tombstones should be cleaned
                await documentDatabase.TombstoneCleaner.ExecuteCleanup();

                using (documentDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var tombstonesCount = documentDatabase.DocumentsStorage.GetTombstonesFrom(context, 0, 0, int.MaxValue).Count();
                    Assert.Equal(0, tombstonesCount);
                }
            }
        }


        private static async Task<UpdatePeriodicBackupOperationResult> SetupBackupAsync(string backupPath, DocumentStore store, BackupType backupType, string taskName)
        {
            var config = new PeriodicBackupConfiguration
            {
                LocalSettings = new LocalSettings
                {
                    FolderPath = backupPath
                },
                Name = taskName,
                FullBackupFrequency = "* */6 * * *",
                IncrementalBackupFrequency = "* */6 * * *",
                BackupType = backupType
            };

            var result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));
            return result;
        }

        private static void RunBackup(long taskId, Raven.Server.Documents.DocumentDatabase documentDatabase, bool forceAlwaysFullBackup = false)
        {
            var periodicBackupRunner = documentDatabase.PeriodicBackupRunner;
            periodicBackupRunner.StartBackupTask(taskId, forceAlwaysFullBackup);

            var cts = new CancellationTokenSource();
            cts.CancelAfter(Debugger.IsAttached ? 100000000 : 10000);
            while (periodicBackupRunner.HasRunningBackups() && cts.IsCancellationRequested == false)
                Thread.Sleep(100);

            if (cts.IsCancellationRequested)
                Assert.False(true, "Timed out waiting for backup. It shouldn't take more than 10 seconds to run, even on slow machine...");
        }
    }
}
