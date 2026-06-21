using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using FastTests.Utils;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.Configuration;
using Raven.Client.Documents.Operations.DataArchival;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.Expiration;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Operations.Refresh;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Exceptions.Commercial;
using Raven.Client.ServerWide.Operations.Configuration;
using Raven.Client.Util;
using Raven.Server.ServerWide.Commands;
using Tests.Infrastructure;
using Xunit;
using BackupConfiguration = Raven.Client.Documents.Operations.Backups.BackupConfiguration;

namespace SlowTests.Issues
{
    public class RavenDB_25225 : RavenTestBase
    {
        public RavenDB_25225(ITestOutputHelper output) : base(output)
        {
        }

        // ----------------------------------------
        // Tests for Data Archival License Limits
        // ----------------------------------------

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.BackupExportImport)]
        public async Task ImportingDisabledDataArchivalCommunityLicense()
        {
            DoNotReuseServer();
            var file = GetTempFileName();
            try
            {
                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.DisableRevisionCompression(Server, store);

                    var config = new DataArchivalConfiguration { Disabled = true, ArchiveFrequencyInSec = 100 };
                    await DataArchivalHelper.SetupDataArchival(store, Server.ServerStore, config);

                    var operation = await store.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
                }

                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.PutLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_COMM);

                    var importOperation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                    await importOperation.WaitForCompletionAsync(TimeSpan.FromMinutes(5));
                }
            }
            finally
            {
                File.Delete(file);
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.BackupExportImport)]
        public async Task ImportingDataArchivalCommunityLicense()
        {
            DoNotReuseServer();
            var file = GetTempFileName();
            try
            {
                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.DisableRevisionCompression(Server, store);

                    var config = new DataArchivalConfiguration { Disabled = false, ArchiveFrequencyInSec = 100 };
                    await DataArchivalHelper.SetupDataArchival(store, Server.ServerStore, config);

                    var operation = await store.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
                }

                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.PutLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_COMM);
                    var exception = await Assert.ThrowsAsync<LicenseLimitException>(async () =>
                    {
                        var importOperation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                        await importOperation.WaitForCompletionAsync(TimeSpan.FromMinutes(5));
                    });
                }
            }
            finally
            {
                File.Delete(file);
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.BackupExportImport)]
        public async Task RestoreDisabledDataArchivalCommunityLicense()
        {
            DoNotReuseServer();
            var backupPath = NewDataPath(suffix: "BackupFolder");
            try
            {
                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.DisableRevisionCompression(Server, store);

                    var config = new DataArchivalConfiguration { Disabled = true, ArchiveFrequencyInSec = 100 };
                    await DataArchivalHelper.SetupDataArchival(store, Server.ServerStore, config);

                    await RunBackup(store, backupPath);
                }

                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.PutLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_COMM);

                    runRestore(store, backupPath);
                }
            }
            finally
            {
                Directory.Delete(backupPath, true);
            }
        }

        // ----------------------------------------
        // Tests for Indexes License Limits
        // ----------------------------------------

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.BackupExportImport | RavenTestCategory.Indexes)]
        public async Task ImportingDisabledAdditionalAssembliesWithCommunityLicense()
        {
            DoNotReuseServer();
            var file = GetTempFileName();
            try
            {
                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.DisableRevisionCompression(Server, store);

                    LicenseHelper.PutIndexWithAdditionalAssemblies(store, IndexState.Disabled);

                    var operation = await store.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
                }

                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.PutLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_COMM);
                    var importOperation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                    await importOperation.WaitForCompletionAsync(TimeSpan.FromMinutes(5));

                    var index = store.Maintenance.Send(new GetIndexOperation("test"));
                    Assert.Null(index);
                }
            }
            finally
            {
                File.Delete(file);
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.BackupExportImport | RavenTestCategory.Indexes)]
        public async Task RestoreDisabledAdditionalAssembliesWithCommunityLicense()
        {
            DoNotReuseServer();
            var backupPath = NewDataPath(suffix: "BackupFolder");
            try
            {
                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.DisableRevisionCompression(Server, store);

                    LicenseHelper.PutIndexWithAdditionalAssemblies(store, IndexState.Disabled);

                    await RunBackup(store, backupPath);
                }

                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.PutLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_COMM);

                    runRestore(store, backupPath);
                }
            }
            finally
            {
                Directory.Delete(backupPath, true);
            }
        }

        // ----------------------------------------
        // Tests for Revision License Limits
        // ----------------------------------------

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.BackupExportImport)]
        public async Task ImportingDisabledRevisionCommunityLicense()
        {
            DoNotReuseServer();
            var file = GetTempFileName();
            try
            {
                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.DisableRevisionCompression(Server, store);

                    var configuration = new RevisionsConfiguration { Default = new RevisionsCollectionConfiguration { Disabled = true, MinimumRevisionsToKeep = 0 } };
                    await store.Maintenance.SendAsync(new ConfigureRevisionsOperation(configuration));

                    var operation = await store.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
                }

                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.PutLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_COMM);
                    var importOperation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                    await importOperation.WaitForCompletionAsync(TimeSpan.FromMinutes(5));
                }
            }
            finally
            {
                File.Delete(file);
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.BackupExportImport)]
        public async Task RestoreDisabledRevisionCommunityLicense()
        {
            DoNotReuseServer();
            var backupPath = NewDataPath(suffix: "BackupFolder");
            try
            {
                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.DisableRevisionCompression(Server, store);

                    var configuration = new RevisionsConfiguration { Default = new RevisionsCollectionConfiguration { Disabled = true, MinimumRevisionsToKeep = 0 } };
                    await store.Maintenance.SendAsync(new ConfigureRevisionsOperation(configuration));

                    await RunBackup(store, backupPath);
                }

                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.PutLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_COMM);

                    runRestore(store, backupPath);
                    WaitForUserToContinueTheTest(store);
                }
            }
            finally
            {
                Directory.Delete(backupPath, true);
            }
        }

        // ----------------------------------------
        // Tests for Backup License Limits
        // ----------------------------------------

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.BackupExportImport)]
        public async Task ImportingDisabledSnapshotWithCommunityLicense()
        {
            DoNotReuseServer();
            var backupPath = NewDataPath(suffix: "BackupFolder");
            var file = GetTempFileName();
            try
            {
                PeriodicBackupConfiguration config;
                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.DisableRevisionCompression(Server, store);

                    config = Backup.CreateBackupConfiguration(backupPath, fullBackupFrequency: "* */1 * * *", incrementalBackupFrequency: "* */2 * * *", backupType: BackupType.Snapshot, disabled: true);
                    await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));

                    var operation = await store.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
                }

                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.PutLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_COMM);

                    var importOperation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                    await importOperation.WaitForCompletionAsync(TimeSpan.FromMinutes(5));
                }
            }
            finally
            {
                File.Delete(file);
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.BackupExportImport)]
        public async Task RestoreDisabledSnapshotWithCommunityLicense()
        {
            DoNotReuseServer();
            var backupPath = NewDataPath(suffix: "BackupFolder");
            try
            {
                PeriodicBackupConfiguration config;
                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.DisableRevisionCompression(Server, store);

                    config = Backup.CreateBackupConfiguration(backupPath, fullBackupFrequency: "* */1 * * *", incrementalBackupFrequency: "* */2 * * *", backupType: BackupType.Snapshot, disabled: true);
                    await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));

                    await RunBackup(store, backupPath);
                }

                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.PutLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_COMM);

                    runRestore(store, backupPath);
                }
            }
            finally
            {
                Directory.Delete(backupPath, true);
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.BackupExportImport)]
        public async Task ImportingDisabledSnapshotWithProLicense()
        {
            DoNotReuseServer();
            var backupPath = NewDataPath(suffix: "BackupFolder");
            var file = GetTempFileName();
            try
            {
                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.DisableRevisionCompression(Server, store);
                    await LicenseHelper.CreatePeriodicBackup(backupPath, store, BackupType.Snapshot, true);

                    var operation = await store.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
                }

                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.ChangeLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_PRO);

                    var importOperation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                    await importOperation.WaitForCompletionAsync(TimeSpan.FromMinutes(5));
                }
            }
            finally
            {
                File.Delete(file);
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.BackupExportImport)]
        public async Task RestoreDisabledSnapshotWithProLicense()
        {
            DoNotReuseServer();
            var backupPath = NewDataPath(suffix: "BackupFolder");
            try
            {
                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.DisableRevisionCompression(Server, store);
                    await LicenseHelper.CreatePeriodicBackup(backupPath, store, BackupType.Snapshot, true);

                    await RunBackup(store, backupPath);
                }

                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.ChangeLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_PRO);

                    runRestore(store, backupPath);
                }
            }
            finally
            {
                Directory.Delete(backupPath, true);
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.BackupExportImport)]
        public async Task ImportingDisabledBackupWithCommunityLicense()
        {
            DoNotReuseServer();
            var backupPath = NewDataPath(suffix: "BackupFolder");
            var file = GetTempFileName();
            try
            {
                PeriodicBackupConfiguration config;
                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.DisableRevisionCompression(Server, store);

                    config = Backup.CreateBackupConfiguration(backupPath, fullBackupFrequency: "* */1 * * *", incrementalBackupFrequency: "* */2 * * *", backupType: BackupType.Backup, disabled: true);
                    await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));

                    var operation = await store.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
                }

                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.PutLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_COMM);

                    var importOperation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                    await importOperation.WaitForCompletionAsync(TimeSpan.FromMinutes(5));
                }
            }
            finally
            {
                File.Delete(file);
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.BackupExportImport)]
        public async Task RestoreDisabledBackupWithCommunityLicense()
        {
            DoNotReuseServer();
            var backupPath = NewDataPath(suffix: "BackupFolder");
            try
            {
                PeriodicBackupConfiguration config;
                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.DisableRevisionCompression(Server, store);

                    config = Backup.CreateBackupConfiguration(backupPath, fullBackupFrequency: "* */1 * * *", incrementalBackupFrequency: "* */2 * * *", backupType: BackupType.Backup, disabled: true);
                    await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));

                    await RunBackup(store, backupPath);
                }

                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.PutLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_COMM);

                    runRestore(store, backupPath);
                }
            }
            finally
            {
                Directory.Delete(backupPath, true);
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.BackupExportImport)]
        public async Task ImportingDisabledBackupWithProLicense()
        {
            DoNotReuseServer();
            var backupPath = NewDataPath(suffix: "BackupFolder");
            var file = GetTempFileName();
            try
            {
                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.DisableRevisionCompression(Server, store);
                    await LicenseHelper.CreatePeriodicBackup(backupPath, store, BackupType.Backup, true);

                    var operation = await store.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
                }

                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.ChangeLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_PRO);

                    var importOperation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                    await importOperation.WaitForCompletionAsync(TimeSpan.FromMinutes(5));
                }
            }
            finally
            {
                File.Delete(file);
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.BackupExportImport)]
        public async Task RestoreDisabledBackupWithProLicense()
        {
            DoNotReuseServer();
            var backupPath = NewDataPath(suffix: "BackupFolder");
            try
            {
                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.DisableRevisionCompression(Server, store);
                    await LicenseHelper.CreatePeriodicBackup(backupPath, store, BackupType.Backup, true);

                    await RunBackup(store, backupPath);
                }

                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.ChangeLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_PRO);

                    runRestore(store, backupPath);
                }
            }
            finally
            {
                Directory.Delete(backupPath, true);
            }
        }

        // ----------------------------------------
        // Tests for Client Configuration License Limits
        //  ----------------------------------------

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.BackupExportImport)]
        public async Task ImportingDisabledClientConfigurationWithCommunityLicense()
        {
            DoNotReuseServer();
            var file = GetTempFileName();
            try
            {
                using (var store = GetDocumentStore(new Options { ModifyDatabaseRecord = r => r.Client = new ClientConfiguration { MaxNumberOfRequestsPerSession = 50, Disabled = true} }))
                {
                    await LicenseHelper.DisableRevisionCompression(Server, store);

                    var operation = await store.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
                }

                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.PutLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_COMM);

                    var importOperation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                    await importOperation.WaitForCompletionAsync(TimeSpan.FromMinutes(5));
                }
            }
            finally
            {
                File.Delete(file);
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.BackupExportImport)]
        public async Task ImportingClientConfigurationWithCommunityLicense()
        {
            DoNotReuseServer();
            var file = GetTempFileName();
            try
            {
                using (var store = GetDocumentStore(new Options { ModifyDatabaseRecord = r => r.Client = new ClientConfiguration { MaxNumberOfRequestsPerSession = 50, Disabled = false } }))
                {
                    await LicenseHelper.DisableRevisionCompression(Server, store);

                    var operation = await store.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
                }

                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.PutLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_COMM);
                    var exception = await Assert.ThrowsAsync<LicenseLimitException>(async () =>
                    {
                        var importOperation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                        await importOperation.WaitForCompletionAsync(TimeSpan.FromMinutes(5));
                    });
                }
            }
            finally
            {
                File.Delete(file);
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.BackupExportImport)]
        public async Task RestoreDisabledClientConfigurationWithCommunityLicense()
        {
            DoNotReuseServer();
            var backupPath = NewDataPath(suffix: "BackupFolder");
            try
            {
                using (var store = GetDocumentStore(new Options { ModifyDatabaseRecord = r => r.Client = new ClientConfiguration { MaxNumberOfRequestsPerSession = 50, Disabled = true } }))
                {
                    await LicenseHelper.DisableRevisionCompression(Server, store);

                    await RunBackup(store, backupPath);
                }

                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.PutLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_COMM);

                    runRestore(store, backupPath);
                }
            }
            finally
            {
                Directory.Delete(backupPath, true);
            }
        }

        // ----------------------------------------
        // Tests for Studio Configuration License Limits
        //  ----------------------------------------

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.BackupExportImport)]
        public async Task ImportingDisabledStudioConfigurationWithCommunityLicense()
        {
            DoNotReuseServer();
            var file = GetTempFileName();
            try
            {
                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.DisableRevisionCompression(Server, store);
                    var command = new PutDatabaseStudioConfigurationCommand(new StudioConfiguration() { DisableAutoIndexCreation = true, Disabled = true }, store.Database,
                        RaftIdGenerator.NewId());
                    await Server.ServerStore.SendToLeaderAsync(command);
                    var operation = await store.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
                }

                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.PutLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_COMM);

                    var importOperation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                    await importOperation.WaitForCompletionAsync(TimeSpan.FromMinutes(5));
                }
            }
            finally
            {
                File.Delete(file);
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.BackupExportImport)]
        public async Task RestoreDisabledStudioConfigurationWithCommunityLicense()
        {
            DoNotReuseServer();
            var backupPath = NewDataPath(suffix: "BackupFolder");
            try
            {
                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.DisableRevisionCompression(Server, store);
                    var command = new PutDatabaseStudioConfigurationCommand(new ServerWideStudioConfiguration() { DisableAutoIndexCreation = true, Disabled = true}, store.Database,
                        RaftIdGenerator.NewId());
                    await Server.ServerStore.SendToLeaderAsync(command);
                    await RunBackup(store, backupPath);
                }

                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.PutLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_COMM);

                    runRestore(store, backupPath);
                }
            }
            finally
            {
                Directory.Delete(backupPath, true);
            }
        }

        // ----------------------------------------
        // Tests for Expiration License Limits
        //  ----------------------------------------

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.BackupExportImport)]
        public async Task ImportingDisabledExpirationWithCommunityLicense()
        {
            DoNotReuseServer();
            var file = GetTempFileName();
            try
            {
                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.DisableRevisionCompression(Server, store);

                    var config = new ExpirationConfiguration { Disabled = true, DeleteFrequencyInSec = 100, };
                    await ExpirationHelper.SetupExpiration(store, Server.ServerStore, config);

                    var operation = await store.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
                }

                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.PutLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_COMM);

                    var importOperation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                    await importOperation.WaitForCompletionAsync(TimeSpan.FromMinutes(5));
                }
            }
            finally
            {
                File.Delete(file);
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.BackupExportImport)]
        public async Task RestoreDisabledExpirationWithCommunityLicense()
        {
            DoNotReuseServer();
            var backupPath = NewDataPath(suffix: "BackupFolder");
            try
            {
                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.DisableRevisionCompression(Server, store);

                    var config = new ExpirationConfiguration { Disabled = true, DeleteFrequencyInSec = 100, };
                    await ExpirationHelper.SetupExpiration(store, Server.ServerStore, config);

                    await RunBackup(store, backupPath);
                }

                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.PutLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_COMM);

                    runRestore(store, backupPath);
                }
            }
            finally
            {
                Directory.Delete(backupPath, true);
            }
        }

        // ----------------------------------------
        // Tests for Refresh License Limits
        //  ----------------------------------------

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.BackupExportImport)]
        public async Task ImportingDisabledRefreshWithCommunityLicense()
        {
            DoNotReuseServer();
            var file = GetTempFileName();
            try
            {
                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.DisableRevisionCompression(Server, store);

                    var refConfig = new RefreshConfiguration { RefreshFrequencyInSec = 33, Disabled = true };
                    await store.Maintenance.SendAsync(new ConfigureRefreshOperation(refConfig));

                    var operation = await store.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
                }

                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.PutLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_COMM);

                    var importOperation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                    await importOperation.WaitForCompletionAsync(TimeSpan.FromMinutes(5));
                }
            }
            finally
            {
                File.Delete(file);
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.BackupExportImport)]
        public async Task RestoreDisabledRefreshWithCommunityLicense()
        {
            DoNotReuseServer();
            var backupPath = NewDataPath(suffix: "BackupFolder");
            try
            {
                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.DisableRevisionCompression(Server, store);

                    var refConfig = new RefreshConfiguration { RefreshFrequencyInSec = 33, Disabled = true };
                    await store.Maintenance.SendAsync(new ConfigureRefreshOperation(refConfig));

                    await RunBackup(store, backupPath);
                }

                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.PutLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_COMM);

                    runRestore(store, backupPath);
                }
            }
            finally
            {
                Directory.Delete(backupPath, true);
            }
        }

        // ----------------------------------------
        // Tests for ETL License Limits
        //  ---------------------------------------

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Etl)]
        public async Task ImportingDisabledRavenEtlWithCommunityLicense()
        {
            DoNotReuseServer();
            var dbName = $"db/{Guid.NewGuid()}";
            var csName = $"cs/{Guid.NewGuid()}";
            var file = GetTempFileName();
            try
            {
                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.DisableRevisionCompression(Server, store);
                    LicenseHelper.CreateRavenEtlConfiguration(csName, dbName, store, out AddEtlOperationResult _, true);

                    var operation = await store.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
                }

                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.ChangeLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_COMM);

                    var importOperation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                    await importOperation.WaitForCompletionAsync(TimeSpan.FromMinutes(5));
                }
            }
            finally
            {
                File.Delete(file);
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Etl)]
        public async Task RestoreDisabledRavenEtlWithCommunityLicense()
        {
            DoNotReuseServer();
            var dbName = $"db/{Guid.NewGuid()}";
            var csName = $"cs/{Guid.NewGuid()}";

            var backupPath = NewDataPath(suffix: "BackupFolder");
            try
            {
                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.DisableRevisionCompression(Server, store);
                    LicenseHelper.CreateRavenEtlConfiguration(csName, dbName, store, out AddEtlOperationResult _, true);
                    WaitForUserToContinueTheTest(store);
                    await RunBackup(store, backupPath);
                }

                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.ChangeLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_COMM);

                    runRestore(store, backupPath);
                }
            }
            finally
            {
                Directory.Delete(backupPath, true);
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Etl)]
        public async Task ImportingDisabledSqlEtlWithCommunityLicense()
        {
            DoNotReuseServer();
            var dbName = $"db/{Guid.NewGuid()}";
            var csName = $"cs/{Guid.NewGuid()}";
            var file = GetTempFileName();
            try
            {
                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.DisableRevisionCompression(Server, store);
                    await LicenseHelper.CreateSqlEtlConfiguration(store, true);

                    var operation = await store.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
                }

                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.ChangeLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_COMM);

                    var importOperation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                    await importOperation.WaitForCompletionAsync(TimeSpan.FromMinutes(5));
                }
            }
            finally
            {
                File.Delete(file);
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Etl)]
        public async Task RestoreDisabledSqlEtlWithCommunityLicense()
        {
            DoNotReuseServer();
            var dbName = $"db/{Guid.NewGuid()}";
            var csName = $"cs/{Guid.NewGuid()}";

            var backupPath = NewDataPath(suffix: "BackupFolder");
            try
            {
                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.DisableRevisionCompression(Server, store);
                    await LicenseHelper.CreateSqlEtlConfiguration(store, true);
                    await RunBackup(store, backupPath);
                }

                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.ChangeLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_COMM);

                    runRestore(store, backupPath);
                }
            }
            finally
            {
                Directory.Delete(backupPath, true);
            }
        }

        // ----------------------------------------
        // Tests for Replication License Limits
        //  ----------------------------------------

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Replication)]
        public async Task ImportingDisabledExternalReplicationWithCommunityLicense()
        {
            DoNotReuseServer();
            using (var server1 = GetNewServer())
            using (var server2 = GetNewServer())
            {
                var file = GetTempFileName();
                var dbName = $"db/{Guid.NewGuid()}";
                var csName = $"cs/{Guid.NewGuid()}";
                try
                {
                    using (var store1 = GetDocumentStore(new Options() { Server = server2 }))
                    using (var store2 = GetDocumentStore(new Options() { Server = server1 }))
                    {
                        await LicenseHelper.DisableRevisionCompression(server2, store1);
                        await LicenseHelper.DisableRevisionCompression(server1, store2);

                        await LicenseHelper.CreateExternalReplication(csName, dbName, store1, true);

                        var operation = await store1.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
                        await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                        await LicenseHelper.ChangeLicenseAndDisableRevisionCompression(server1, store2, LicenseTestBase.RL_COMM);

                        var importOperation = await store2.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                        await importOperation.WaitForCompletionAsync(TimeSpan.FromMinutes(5));
                    }
                }
                finally
                {
                    File.Delete(file);
                }
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Replication)]
        public async Task RestoreDisabledExternalReplicationWithCommunityLicense()
        {
            DoNotReuseServer();
            var backupPath = NewDataPath(suffix: "BackupFolder");
            using (var server1 = GetNewServer())
            using (var server2 = GetNewServer())
            {
                var dbName = $"db/{Guid.NewGuid()}";
                var csName = $"cs/{Guid.NewGuid()}";
                try
                {
                    using (var store1 = GetDocumentStore(new Options() { Server = server2 }))
                    using (var store2 = GetDocumentStore(new Options() { Server = server1 }))
                    {
                        await LicenseHelper.DisableRevisionCompression(server2, store1);
                        await LicenseHelper.DisableRevisionCompression(server1, store2);

                        await LicenseHelper.CreateExternalReplication(csName, dbName, store1, true);

                        await RunBackup(store1, backupPath);

                        await LicenseHelper.ChangeLicenseAndDisableRevisionCompression(server1, store2, LicenseTestBase.RL_COMM);

                        runRestore(store2, backupPath);
                    }
                }
                finally
                {
                    Directory.Delete(backupPath, true);
                }
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Replication)]
        public async Task ImportingDisabledDelayedExternalReplicationWithProLicense()
        {
            DoNotReuseServer();
            using (var server1 = GetNewServer())
            using (var server2 = GetNewServer())
            {
                var file = GetTempFileName();
                var dbName = $"cs/{Guid.NewGuid()}";
                var csName = $"cs/{Guid.NewGuid()}";
                try
                {
                    using (var store1 = GetDocumentStore(new Options() { Server = server2 }))
                    using (var store2 = GetDocumentStore(new Options() { Server = server1 }))
                    {
                        await LicenseHelper.DisableRevisionCompression(server2, store1);
                        await LicenseHelper.DisableRevisionCompression(server1, store2);

                        await LicenseHelper.CreateExternalReplication(csName, dbName, store1, true);

                        var operation = await store1.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
                        await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                        await LicenseHelper.ChangeLicenseAndDisableRevisionCompression(server1, store2, LicenseTestBase.RL_COMM);

                        var importOperation = await store2.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                        await importOperation.WaitForCompletionAsync(TimeSpan.FromMinutes(5));
                    }
                }
                finally
                {
                    File.Delete(file);
                }
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Replication)]
        public async Task RestoreDisabledDelayedExternalReplicationWithProLicense()
        {
            DoNotReuseServer();
            var backupPath = NewDataPath(suffix: "BackupFolder");
            using (var server1 = GetNewServer())
            using (var server2 = GetNewServer())
            {
                var dbName = $"cs/{Guid.NewGuid()}";
                var csName = $"cs/{Guid.NewGuid()}";
                try
                {
                    using (var store1 = GetDocumentStore(new Options() { Server = server2 }))
                    using (var store2 = GetDocumentStore(new Options() { Server = server1 }))
                    {
                        await LicenseHelper.DisableRevisionCompression(server2, store1);
                        await LicenseHelper.DisableRevisionCompression(server1, store2);

                        await LicenseHelper.CreateExternalReplication(csName, dbName, store1, true);

                        await RunBackup(store1, backupPath);

                        await LicenseHelper.ChangeLicenseAndDisableRevisionCompression(server1, store2, LicenseTestBase.RL_COMM);

                        runRestore(store2, backupPath);
                    }
                }
                finally
                {
                    Directory.Delete(backupPath, true);
                }
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Replication)]
        public async Task ImportingDisabledPullReplicationAsSinkWithCommunityLicense()
        {
            DoNotReuseServer();

            var file = GetTempFileName();
            var dbName = $"db/{Guid.NewGuid()}";
            var csName = $"cs/{Guid.NewGuid()}";
            try
            {
                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.DisableRevisionCompression(Server, store);

                    await LicenseHelper.CreatePullReplicationAsSink(dbName, csName, store, true);

                    var operation = await store.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
                }

                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.ChangeLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_COMM);
                    var importOperation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                    await importOperation.WaitForCompletionAsync(TimeSpan.FromMinutes(5));
                }
            }
            finally
            {
                File.Delete(file);
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Replication)]
        public async Task RestoreDisabledPullReplicationAsSinkWithCommunityLicense()
        {
            DoNotReuseServer();

            var backupPath = NewDataPath(suffix: "BackupFolder");
            var dbName = $"db/{Guid.NewGuid()}";
            var csName = $"cs/{Guid.NewGuid()}";
            try
            {
                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.DisableRevisionCompression(Server, store);

                    await LicenseHelper.CreatePullReplicationAsSink(dbName, csName, store, true);

                    await RunBackup(store, backupPath);
                }

                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.ChangeLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_COMM);
                    runRestore(store, backupPath);
                }
            }
            finally
            {
                Directory.Delete(backupPath, true);
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Replication)]
        public async Task ImportingDisabledPullReplicationAsHubWithCommunityLicense()
        {
            DoNotReuseServer();

            var file = GetTempFileName();
            try
            {
                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.DisableRevisionCompression(Server, store);
                    LicenseHelper.CraetePullReplicationDefinition(store, true);

                    var operation = await store.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
                }

                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.ChangeLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_COMM);

                    var importOperation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                    await importOperation.WaitForCompletionAsync(TimeSpan.FromMinutes(5));
                }
            }
            finally
            {
                File.Delete(file);
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Replication)]
        public async Task RestoreDisabledPullReplicationAsHubWithCommunityLicense()
        {
            DoNotReuseServer();

            var backupPath = NewDataPath(suffix: "BackupFolder");
            try
            {
                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.DisableRevisionCompression(Server, store);
                    LicenseHelper.CraetePullReplicationDefinition(store, true);

                    await RunBackup(store, backupPath);
                }

                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.ChangeLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_COMM);

                    runRestore(store, backupPath);
                }
            }
            finally
            {
                Directory.Delete(backupPath, true);
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Replication)]
        public async Task ImportingDisabledPullReplicationAsHubWithProLicense()
        {
            DoNotReuseServer();

            var file = GetTempFileName();
            try
            {
                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.DisableRevisionCompression(Server, store);
                    LicenseHelper.CraetePullReplicationDefinition(store, true);

                    var operation = await store.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
                }

                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.ChangeLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_PRO);

                    var importOperation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                    await importOperation.WaitForCompletionAsync(TimeSpan.FromMinutes(5));
                }
            }
            finally
            {
                File.Delete(file);
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Replication)]
        public async Task RestoreDisabledPullReplicationAsHubWithProLicense()
        {
            DoNotReuseServer();

            var backupPath = NewDataPath(suffix: "BackupFolder");
            try
            {
                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.DisableRevisionCompression(Server, store);
                    LicenseHelper.CraetePullReplicationDefinition(store, true);

                    await RunBackup(store, backupPath);
                }

                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.ChangeLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_PRO);

                    runRestore(store, backupPath);
                }
            }
            finally
            {
                Directory.Delete(backupPath, true);
            }
        }

        // ----------------------------------------
        // Tests for Time Series Configuration License Limits
        //  ----------------------------------------

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.TimeSeries)]
        public async Task ImportingDisabledTsRollupAndRetentionWithCommunityLicense()
        {
            DoNotReuseServer();

            var file = GetTempFileName();
            try
            {
                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.DisableRevisionCompression(Server, store);
                    LicenseHelper.CreateTsRollupAndRetention(store, true);

                    var operation = await store.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
                }

                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.ChangeLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_COMM);

                    var importOperation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                    await importOperation.WaitForCompletionAsync(TimeSpan.FromMinutes(5));
                }
            }
            finally
            {
                File.Delete(file);
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.TimeSeries)]
        public async Task RestoreDisabledTsRollupAndRetentionWithCommunityLicense()
        {
            DoNotReuseServer();
            var backupPath = NewDataPath(suffix: "BackupFolder");
            try
            {
                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.DisableRevisionCompression(Server, store);
                    LicenseHelper.CreateTsRollupAndRetention(store, true);

                    await RunBackup(store, backupPath);
                }

                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.ChangeLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_COMM);

                    runRestore(store, backupPath);
                }
            }
            finally
            {
                Directory.Delete(backupPath, true);
            }
        }

        private void runRestore(DocumentStore store, string backupPath)
        {
            var configuration = new RestoreBackupConfiguration { DatabaseName = store.Database + "1" };
            configuration.BackupLocation = Directory.GetDirectories(backupPath).First();
           Backup.RestoreDatabase(store, configuration);
        }

        private static async Task RunBackup(DocumentStore store, string backupPath)
        {
            var operation = await store.Maintenance.SendAsync(new BackupOperation(new BackupConfiguration
            {
                BackupType = BackupType.Backup,
                LocalSettings = new LocalSettings
                {
                    FolderPath = backupPath
                }
            }));

            _ = (BackupResult)await operation.WaitForCompletionAsync(TimeSpan.FromSeconds(30));
        }
    }
}
