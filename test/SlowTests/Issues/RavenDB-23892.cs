using System;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Operations.Configuration;
using Raven.Client.ServerWide.Operations.OngoingTasks;
using SlowTests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_23892 : RavenTestBase
    {
        public RavenDB_23892(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.BackupExportImport)]
        public async Task Snapshot_Should_Not_Include_ServerWideBackupConfiguration()
        {
            using (var server = GetNewServer())
            using (var store = GetDocumentStore(new Options { Server = server }))
            {
                var database = store.Database;
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Toli" }, "users/1");
                    await session.SaveChangesAsync();
                }
                var serverWideConfig = new ServerWideBackupConfiguration
                {
                    Name = "ServerWideBackup",
                    Disabled = false,
                    BackupType = BackupType.Backup,
                    FullBackupFrequency = "0 1 * * *",
                    LocalSettings = new LocalSettings { FolderPath = "test/folder" }
                };
                var result = await store.Maintenance.Server.SendAsync(new PutServerWideBackupConfigurationOperation(serverWideConfig));
                var backupPath = NewDataPath(suffix: "BackupFolder");
                var config = Backup.CreateBackupConfiguration(backupPath, backupType: BackupType.Snapshot);
                config.SnapshotSettings = new SnapshotSettings { CompressionLevel = CompressionLevel.Fastest, ExcludeIndexes = false };
                await Backup.UpdateConfigAndRunBackupAsync(server, config, store);
                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));

                Assert.Equal(2, record.PeriodicBackups.Count);
                bool exists = record.PeriodicBackups.Exists(x => x.Name.Contains(serverWideConfig.Name));
                Assert.True(exists);

                var databaseName = $"restored_database-{Guid.NewGuid()}";
                await store.Maintenance.Server.SendAsync(new DeleteServerWideTaskOperation(result.Name, OngoingTaskType.Backup));
                record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                exists = record.PeriodicBackups.Exists(x => x.Name.Contains(serverWideConfig.Name));
                Assert.False(exists);

                using (Backup.RestoreDatabase(store,
                           new RestoreBackupConfiguration { BackupLocation = Directory.GetDirectories(backupPath).First(), DatabaseName = databaseName }))
                {
                    record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(databaseName));
                    exists = record.PeriodicBackups.Exists(x => x.Name.Contains(serverWideConfig.Name));
                    Assert.False(exists);
                }
            }
        }

        [RavenFact(RavenTestCategory.BackupExportImport | RavenTestCategory.Replication)]
        public async Task Snapshot_Should_Not_Include_ServerWideExternalReplication()
        {
            using (var server = GetNewServer())
            using (var store = GetDocumentStore(new Options { Server = server }))
            {
                var database = store.Database;

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Toli" }, "users/1");
                    await session.SaveChangesAsync();
                }

                var externalReplication = new ServerWideExternalReplication
                {
                    Disabled = false,
                    TopologyDiscoveryUrls = new[] { store.Urls.First() },
                    Name = store.Database
                };

                await store.Maintenance.Server.SendAsync(new PutServerWideExternalReplicationOperation(externalReplication));
                WaitForUserToContinueTheTest(store);
                var backupPath = NewDataPath(suffix: "SnapshotReplication");
                var config = Backup.CreateBackupConfiguration(backupPath, BackupType.Snapshot);
                config.SnapshotSettings = new SnapshotSettings
                {
                    CompressionLevel = CompressionLevel.Fastest,
                    ExcludeIndexes = false
                };

                await Backup.UpdateConfigAndRunBackupAsync(server, config, store);

                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(database));
                Assert.Contains(record.ExternalReplications, x => x.Name.Contains(externalReplication.Name));

                // Delete the server-wide replication
                await store.Maintenance.Server.SendAsync(new DeleteServerWideTaskOperation(externalReplication.Name, OngoingTaskType.Replication));

                record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(database));
                Assert.DoesNotContain(record.ExternalReplications, x => x.Name.Contains(externalReplication.Name));

                var restoredDatabase = $"restored-db-{Guid.NewGuid()}";

                using (Backup.RestoreDatabase(store, new RestoreBackupConfiguration
                {
                    BackupLocation = Directory.GetDirectories(backupPath).First(),
                    DatabaseName = restoredDatabase
                }))
                {
                    record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(restoredDatabase));
                    Assert.DoesNotContain(record.ExternalReplications, x => x.Name.Contains(externalReplication.Name));
                }
            }
        }
    }
}
