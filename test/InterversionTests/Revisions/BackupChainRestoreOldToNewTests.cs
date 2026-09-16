using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Config;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using static InterversionTests.Revisions.RevisionsInterversionHelpers;

namespace InterversionTests.Revisions
{
    public class BackupChainRestoreOldToNewTests : OldDataFixture
    {
        public BackupChainRestoreOldToNewTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenMultiplatformFact(RavenTestCategory.Revisions | RavenTestCategory.BackupExportImport | RavenTestCategory.Interversion, RavenPlatform.Windows | RavenPlatform.Linux)]
        public async Task IncrementalBackupChain_OldToNew_AllPhasesPreserved()
        {
            var customSettings = new Dictionary<string, string>
            {
                [RavenConfiguration.GetKey(x => x.Core.RunInMemory)] = "false",
                [RavenConfiguration.GetKey(x => x.Licensing.EulaAccepted)] = "true",
            };

            var oldNode = await GetServerAsync(Versions.PrePRv62, customSettings: customSettings);

            var seedDb = GetDatabaseName() + "-seed";
            using var seedStore = new DocumentStore { Urls = new[] { oldNode.Url }, Database = seedDb };
            seedStore.Initialize();

            await seedStore.Maintenance.Server.SendAsync(new CreateDatabaseOperation(new DatabaseRecord(seedDb)
            {
                Settings = { [RavenConfiguration.GetKey(x => x.Core.RunInMemory)] = "false" }
            }));

            await seedStore.Maintenance.SendAsync(new ConfigureRevisionsOperation(new RevisionsConfiguration
            {
                Default = new RevisionsCollectionConfiguration
                {
                    Disabled = false,
                    MinimumRevisionsToKeep = 100,
                    PurgeOnDelete = false
                }
            }));

            var backupPath = NewDataPath(suffix: "chain-backup");
            var backupConfig = new PeriodicBackupConfiguration
            {
                Name = "ChainBackup",
                LocalSettings = new LocalSettings { FolderPath = backupPath },
                BackupType = BackupType.Backup,
                FullBackupFrequency = "0 0 1 1 *",       // yearly placeholder; trigger manually
                IncrementalBackupFrequency = "0 0 1 1 *"
            };
            var taskResult = await seedStore.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(backupConfig));

            using (var session = seedStore.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = "phase1" }, "users/1");
                await session.SaveChangesAsync();
            }
            var phase1Cv = await GetLatestRevisionCvAsync(seedStore, "users/1");

            await seedStore.Maintenance.SendAsync(new StartBackupOperation(isFullBackup: true, taskResult.TaskId));
            await WaitForFullBackupAsync(seedStore, taskResult.TaskId);

            using (var session = seedStore.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = "phase2-new" }, "users/2");
                var u = await session.LoadAsync<User>("users/1");
                u.Name = "phase2-updated";
                await session.SaveChangesAsync();
            }
            var phase2DocBCv = await GetLatestRevisionCvAsync(seedStore, "users/2");
            var phase2DocACv = await GetLatestRevisionCvAsync(seedStore, "users/1");

            var lastIncAt = await GetLastIncrementalBackupAsync(seedStore, taskResult.TaskId);
            await seedStore.Maintenance.SendAsync(new StartBackupOperation(isFullBackup: false, taskResult.TaskId));
            await WaitForIncrementalBackupAsync(seedStore, taskResult.TaskId, after: lastIncAt);

            using (var session = seedStore.OpenAsyncSession())
            {
                session.Delete("users/1");
                await session.SaveChangesAsync();
            }
            var phase3DocADeleteCv = await GetLatestRevisionCvAsync(seedStore, "users/1");

            lastIncAt = await GetLastIncrementalBackupAsync(seedStore, taskResult.TaskId);
            await seedStore.Maintenance.SendAsync(new StartBackupOperation(isFullBackup: false, taskResult.TaskId));
            await WaitForIncrementalBackupAsync(seedStore, taskResult.TaskId, after: lastIncAt);

            KillSlavedServerProcess(oldNode.Process);

            using var hostStore = GetCurrentBitsStore();
            var restoreDb = GetDatabaseName() + "-restored";
            using var __ = Databases.EnsureDatabaseDeletion(restoreDb, hostStore);

            var backupSubdir = Directory.GetDirectories(backupPath).FirstOrDefault()
                ?? throw new InvalidOperationException("No backup subdirectory present.");

            using var restoredStore = await RestoreAsync(hostStore, restoreDb, backupSubdir);

            // Cumulative CV set is what matters (count may vary by 1 due to a HasRevisions-flag-transition revision).
            using (var session = restoredStore.OpenAsyncSession())
            {
                var md1 = await session.Advanced.Revisions.GetMetadataForAsync("users/1", pageSize: 100);
                Assert.True(md1.Count >= 3, $"users/1: expected >= 3 revisions, got {md1.Count}");

                var phase3Flags = md1[0].GetString("@flags") ?? "";
                Assert.Contains("DeleteRevision", phase3Flags);

                var cvs = md1.Select(m => m.GetString("@change-vector")).ToList();
                Assert.Contains(phase1Cv, cvs);
                Assert.Contains(phase2DocACv, cvs);
                Assert.Contains(phase3DocADeleteCv, cvs);
            }

            using (var session = restoredStore.OpenAsyncSession())
            {
                var md2 = await session.Advanced.Revisions.GetMetadataForAsync("users/2", pageSize: 100);
                Assert.True(md2.Count >= 1, $"users/2: expected >= 1 revision, got {md2.Count}");
                var cvs = md2.Select(m => m.GetString("@change-vector")).ToList();
                Assert.Contains(phase2DocBCv, cvs);
            }
        }
    }
}
