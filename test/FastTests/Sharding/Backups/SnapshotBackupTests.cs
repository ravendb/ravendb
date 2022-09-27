using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Exceptions.Sharding;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Sharding.Backups;

public class SnapshotBackupTests : RavenTestBase
{
    public SnapshotBackupTests(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
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
                }, store);
            });
        }
    }

    [Fact]
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
