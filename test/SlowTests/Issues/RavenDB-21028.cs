using System.Threading.Tasks;
using Xunit.Abstractions;
using System;
using System.IO;
using FastTests;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Exceptions;
using Raven.Server.Documents.PeriodicBackup;
using SlowTests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_21028 : RavenTestBase
    {

        public RavenDB_21028(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Smuggler)]
        public async Task WaitForCompletionShouldNotHangOnFailureDuringExport()
        {
            using var store = GetDocumentStore();
            await store.Maintenance.SendAsync(new CreateSampleDataOperation());

            var file = GetTempFileName();
            var op = await store.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions { EncryptionKey = "fakeKey" }, file);

            var e = await Assert.ThrowsAnyAsync<Exception>(async () => await op.WaitForCompletionAsync(TimeSpan.FromSeconds(10)));

            Assert.True(e is RavenException or IOException);

        }

        [RavenFact(RavenTestCategory.Smuggler)]
        public async Task WaitForCompletionShouldRespectTimeout()
        {
            UseNewLocalServer();

            using var store = GetDocumentStore();
            using (var session = store.OpenAsyncSession())
            {
                for (int i = 0; i < 5; i++)
                {
                    await session.StoreAsync(new User());
                }

                await session.SaveChangesAsync();
            }

            var path = NewDataPath();
            var config = new BackupConfiguration
            {
                BackupType = BackupType.Backup,
                LocalSettings = new LocalSettings
                {
                    FolderPath = path
                }
            };

            var db = await GetDatabase(store.Database);

            // hold backup execution 
            var tcs = new TaskCompletionSource<object>();

            try
            {
                db.PeriodicBackupRunner.ForTestingPurposesOnly().OnBackupTaskRunHoldBackupExecution = tcs;

                // wait for completion should not hang
                var operation = await store.Maintenance.SendAsync(new BackupOperation(config));
                await Assert.ThrowsAsync<TimeoutException>(async () => await operation.WaitForCompletionAsync(TimeSpan.FromSeconds(5)));
            }
            finally
            {
                tcs.TrySetResult(null);
            }
        }
    }
}
