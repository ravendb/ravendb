using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using NodaTime.Extensions;
using Raven.Client.Documents;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.PeriodicBackup;
using Raven.Server.ServerWide.Context;
using Xunit;

namespace FastTests.Issues
{
    public class RavenDB_8369 : ReplicationTestBase
    {
        public class FooBar
        {
            public string Foo { get; set; }
        }

        [Fact]
        public async Task Tombstones_should_be_cleaned_only_after_backup()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");
            using (var store = GetDocumentStore())
            {
                var result = await SetupBackupAsync(backupPath, store);

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
                
                await documentDatabase.DocumentTombstoneCleaner.ExecuteCleanup();

                using (documentDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var tombstones = documentDatabase.DocumentsStorage.GetTombstonesFrom(context, 0, 0, int.MaxValue).ToList();

                    //since we didn't backup the tombstones yet, they will not get cleaned
                    Assert.Equal(2, tombstones.Count);
                }

                RunBackup(result.TaskId, documentDatabase);

                //force tombstone cleanup - now, after backup, tombstones should be cleaned
                await documentDatabase.DocumentTombstoneCleaner.ExecuteCleanup(); 

                using (documentDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var tombstones = documentDatabase.DocumentsStorage.GetTombstonesFrom(context, 0, 0, int.MaxValue).ToList();
                    Assert.Empty(tombstones);
                }
            }
        }

        private static async Task<UpdatePeriodicBackupOperationResult> SetupBackupAsync(string backupPath, DocumentStore store)
        {
            var config = new PeriodicBackupConfiguration
            {
                LocalSettings = new LocalSettings
                {
                    FolderPath = backupPath
                },
                FullBackupFrequency = "* */1 * * *",
                IncrementalBackupFrequency = "* */2 * * *"
            };

            var result = await store.Admin.Server.SendAsync(new UpdatePeriodicBackupOperation(config, store.Database));
            return result;
        }

        private static void RunBackup(long taskId, Raven.Server.Documents.DocumentDatabase documentDatabase)
        {
            var periodicBackupRunner = documentDatabase.PeriodicBackupRunner;
            periodicBackupRunner.StartBackupTask(taskId, false);

            var cts = new CancellationTokenSource();
            cts.CancelAfter(Debugger.IsAttached ? 100000000 : 10000);
            while (periodicBackupRunner.HasRunningBackups() && cts.IsCancellationRequested == false)
                Thread.Sleep(100);

            if (cts.IsCancellationRequested)
                Assert.False(true, "Timed out waiting for backup. It shouldn't take more than 10 seconds to run, even on slow machine...");
        }
    }
}
