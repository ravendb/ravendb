using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Smuggler;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.PeriodicBackup
{
    public class RavenDB_20544 : RavenTestBase
    {
        public RavenDB_20544(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task CanRunTwoBackupsConcurrently()
        {
            DoNotReuseServer();
            await Server.ServerStore.EnsureNotPassiveAsync();

            using var store = GetDocumentStore();
            await using (var bulk = store.BulkInsert())
            {
                for (int i = 0; i < 10_000; i++)
                {
                    await bulk.StoreAsync(new User(), "users/" + i);
                }
            }

            var backupPath = NewDataPath(suffix: "BackupFolder");
            var config1 = Backup.CreateBackupConfiguration(backupPath, fullBackupFrequency: "* * * * *");
            var config2 = Backup.CreateBackupConfiguration(backupPath, backupType: BackupType.Snapshot);

            var task1 = Backup.UpdateConfigAndRunBackupAsync(Server, config1, store);
            var task2 = Backup.UpdateConfigAndRunBackupAsync(Server, config2, store);

            await Task.WhenAll(task1, task2);

            var backupDirectories = Directory.GetDirectories(backupPath);
            Assert.Equal(2, backupDirectories.Length);

            string[] files = Directory.GetFiles(backupPath, "*.*", SearchOption.AllDirectories);
            Assert.Equal(2, files.Length);

            Assert.Single(files.Where(BackupUtils.IsBackupFile));
            Assert.Single(files.Where(x => BackupUtils.IsSnapshot(Path.GetExtension(x))));
        }

    }
}
