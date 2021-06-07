using System.Linq;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client.Documents.Operations.Backups;
using Raven.Server.ServerWide.Context;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_8369 : ReplicationTestBase
    {
        public RavenDB_8369(ITestOutputHelper output) : base(output)
        {
        }

        public class FooBar
        {
            public string Foo { get; set; }
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task Tombstones_should_be_cleaned_only_after_backup_with_binary_backupwhen_delete_is_last_operation()
        {
            await Tombstones_should_be_cleaned_only_after_backup_when_delete_is_last_operation(BackupType.Backup, "My backup");
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task Tombstones_should_be_cleaned_only_after_backup_with_snapshot_backupwhen_delete_is_last_operation()
        {
            await Tombstones_should_be_cleaned_only_after_backup_when_delete_is_last_operation(BackupType.Snapshot, "My backup");
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task Tombstones_should_be_cleaned_only_after_backup_with_binary_backupwhen_delete_is_not_last_operation()
        {
            await Tombstones_should_be_cleaned_only_after_backup_when_delete_is_not_a_last_operation(BackupType.Backup, "My backup");
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task Tombstones_should_be_cleaned_only_after_backup_with_snapshot_backupwhen_delete_is_not_last_operation()
        {
            await Tombstones_should_be_cleaned_only_after_backup_when_delete_is_not_a_last_operation(BackupType.Snapshot, "My backup");
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task Tomstones_should_be_cleaned_properly_for_multiple_backup_tasks_for_snapshot()
        {
            await Tomstones_should_be_cleaned_properly_for_multiple_backup_tasks(BackupType.Snapshot);
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task Tomstones_should_be_cleaned_properly_for_multiple_backup_tasks_for_backup()
        {
            await Tomstones_should_be_cleaned_properly_for_multiple_backup_tasks(BackupType.Backup);
        }

        private async Task Tomstones_should_be_cleaned_properly_for_multiple_backup_tasks(BackupType backupType)
        {
            var backupPath1 = NewDataPath(suffix: "BackupFolder");
            var backupPath2 = NewDataPath(suffix: "BackupFolder");
            using (var store = GetDocumentStore())
            {
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
                var config1 = Backup.CreateBackupConfiguration(backupPath1, backupType: backupType, incrementalBackupFrequency: "* */6 * * *");
                var config2 = Backup.CreateBackupConfiguration(backupPath2, backupType: backupType, incrementalBackupFrequency: "* */6 * * *");
                var backupTaskId1 = await Backup.UpdateConfigAndRunBackupAsync(Server, config1, store, isFullBackup: false);
                await Backup.UpdateConfigAndRunBackupAsync(Server, config2, store, isFullBackup: false);

                //force tombstone cleanup - now, after backup, tombstones should be cleaned
                await documentDatabase.TombstoneCleaner.ExecuteCleanup(1);

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

                await Backup.RunBackupAsync(Server, backupTaskId1, store, isFullBackup: false);
                await documentDatabase.TombstoneCleaner.ExecuteCleanup(1);

                //since we ran only one of backup tasks, only tombstones with minimal last etag get cleaned
                using (documentDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var tombstonesCount = documentDatabase.DocumentsStorage.GetTombstonesFrom(context, 0, 0, int.MaxValue).Count();
                    Assert.Equal(1, tombstonesCount);
                }
            }
        }

        private async Task Tombstones_should_be_cleaned_only_after_backup_when_delete_is_last_operation(BackupType backupType, string taskName)
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");
            using (var store = GetDocumentStore())
            {
                var config = Backup.CreateBackupConfiguration(backupPath, backupType: backupType, incrementalBackupFrequency: "* */6 * * *");
                var result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));

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

                await documentDatabase.TombstoneCleaner.ExecuteCleanup(1);

                using (documentDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var tombstones = documentDatabase.DocumentsStorage.GetTombstonesFrom(context, 0, 0, int.MaxValue).ToList();

                    //since we didn't backup the tombstones yet, they will not get cleaned
                    Assert.Equal(2, tombstones.Count);
                }

                await Backup.RunBackupAsync(Server, result.TaskId, store, isFullBackup: false);

                //force tombstone cleanup - now, after backup, tombstones should be cleaned
                await documentDatabase.TombstoneCleaner.ExecuteCleanup(1);

                using (documentDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var tombstonesCount = documentDatabase.DocumentsStorage.GetTombstonesFrom(context, 0, 0, int.MaxValue).Count();
                    Assert.Equal(0, tombstonesCount);
                }
            }
        }

        private async Task Tombstones_should_be_cleaned_only_after_backup_when_delete_is_not_a_last_operation(BackupType backupType, string taskName)
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");
            using (var store = GetDocumentStore())
            {
                var config = Backup.CreateBackupConfiguration(backupPath, backupType: backupType, incrementalBackupFrequency: "* */6 * * *");
                var result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));

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

                await documentDatabase.TombstoneCleaner.ExecuteCleanup(1);

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

                await Backup.RunBackupAsync(Server, result.TaskId, store, isFullBackup: false);

                //force tombstone cleanup - now, after backup, tombstones should be cleaned
                await documentDatabase.TombstoneCleaner.ExecuteCleanup(1);

                using (documentDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var tombstonesCount = documentDatabase.DocumentsStorage.GetTombstonesFrom(context, 0, 0, int.MaxValue).Count();
                    Assert.Equal(0, tombstonesCount);
                }
            }
        }
    }
}
