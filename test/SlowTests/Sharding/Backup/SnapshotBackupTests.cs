using FastTests;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Exceptions.Sharding;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Sharding.Backup;

public class SnapshotBackupTests : RavenTestBase
{
    public SnapshotBackupTests(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.BackupExportImport | RavenTestCategory.Sharding)]
    public void Snapshot_Backup_In_Sharded_Database_Should_Throw()
    {
        using (var store = Sharding.GetDocumentStore())
        {
            Assert.Throws<NotSupportedInShardingException>(() =>
            {
                Backup.CreateAndRunBackupInCluster(new PeriodicBackupConfiguration
                {
                    BackupType = BackupType.Snapshot,
                    FullBackupFrequency = "* */1 * * *",
                    LocalSettings = new LocalSettings
                    {
                        FolderPath = NewDataPath()
                    }
                }, store, nodes: null);
            });
        }
    }

    [RavenFact(RavenTestCategory.BackupExportImport | RavenTestCategory.Sharding)]
    public void OneTime_Snapshot_Backup_In_Sharded_Database_Should_Throw()
    {
        using (var store = Sharding.GetDocumentStore())
        {
            Assert.Throws<NotSupportedInShardingException>(() =>
            {
                store.Maintenance.Send(new BackupOperation(new BackupConfiguration
                {
                    BackupType = BackupType.Snapshot,
                    LocalSettings = new LocalSettings
                    {
                        FolderPath = NewDataPath()
                    }
                }));
            });
        }
    }
}
