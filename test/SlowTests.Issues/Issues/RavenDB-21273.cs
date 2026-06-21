using System;
using System.IO;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Exceptions.Commercial;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_21273 : RavenTestBase
    {

        public RavenDB_21273(ITestOutputHelper output) : base(output)
        {
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Smuggler | RavenTestCategory.Compression, Architecture = RavenArchitecture.AllX64)]
        public async Task ExceptionWhenImportingAdditionalAssembliesWithCommunityLicense()
        {
            DoNotReuseServer();
            var file = GetTempFileName();
            try
            {
                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.DisableRevisionCompression(Server, store);

                    LicenseHelper.PutIndexWithAdditionalAssemblies(store);

                    var operation = await store.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
                }

                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.ChangeLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_COMM);

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

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.BackupExportImport)]
        public async Task ExceptionWhenImportingSnapshotWithCommunityLicense()
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

                    config = await LicenseHelper.CreatePeriodicBackup(backupPath, store, BackupType.Snapshot);

                    var operation = await store.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
                }

                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.ChangeLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_COMM);

                    var importOperation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                    await importOperation.WaitForCompletionAsync(TimeSpan.FromMinutes(5));
                    config.Disabled = false;
                    var exception = await Assert.ThrowsAsync<LicenseLimitException>(async () =>
                    {
                        await Backup.UpdateConfigAsync(Server, config, store);
                    });
                    Assert.Equal(LimitType.SnapshotBackup, exception.LimitType);
                }
            }
            finally
            {
                File.Delete(file);
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.BackupExportImport)]
        public async Task ExceptionWhenImportingSnapshotWithProLicense()
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
                    config = await LicenseHelper.CreatePeriodicBackup(backupPath, store, BackupType.Snapshot);

                    var operation = await store.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
                }

                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.ChangeLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_PRO);

                    var importOperation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                    await importOperation.WaitForCompletionAsync(TimeSpan.FromMinutes(5));
                    config.Disabled = false;
                    var exception = await Assert.ThrowsAsync<LicenseLimitException>(async () =>
                    {
                        await Backup.UpdateConfigAsync(Server, config, store);

                    });
                    Assert.Equal(LimitType.SnapshotBackup, exception.LimitType);
                }
            }
            finally
            {
                File.Delete(file);
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Replication)]
        public async Task ExceptionWhenImportingExternalReplicationWithCommunityLicense()
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

                        ExternalReplication watcher = await LicenseHelper.CreateExternalReplication(csName, dbName, store1);

                        var operation = await store1.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
                        await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                        await LicenseHelper.ChangeLicenseAndDisableRevisionCompression(server1, store2, LicenseTestBase.RL_COMM);

                        var importOperation = await store2.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                        await importOperation.WaitForCompletionAsync(TimeSpan.FromMinutes(5));
                        var exception = await Assert.ThrowsAsync<LicenseLimitException>(async () =>
                        {
                            watcher.Disabled = false;
                            await store2.Maintenance.SendAsync(new UpdateExternalReplicationOperation(watcher));
                        });
                        Assert.Equal(LimitType.ExternalReplication, exception.LimitType);
                    }
                }
                finally
                {
                    File.Delete(file);
                }
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Replication)]
        public async Task ExceptionWhenImportingDelayedExternalReplicationWithProLicense()
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

                        ExternalReplication watcher = await LicenseHelper.CreateExternalReplication(csName, dbName, store1);

                        var operation = await store1.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
                        await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                        await LicenseHelper.ChangeLicenseAndDisableRevisionCompression(server1, store2, LicenseTestBase.RL_COMM);

                        var importOperation = await store2.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                        await importOperation.WaitForCompletionAsync(TimeSpan.FromMinutes(5));

                        var exception = await Assert.ThrowsAsync<LicenseLimitException>(async () =>
                        {
                            watcher.Disabled = false;
                            await store2.Maintenance.SendAsync(new UpdateExternalReplicationOperation(watcher));
                        });
                        Assert.Equal(LimitType.ExternalReplication, exception.LimitType);
                    }
                }
                finally
                {
                    File.Delete(file);
                }
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.TimeSeries)]
        public async Task ExceptionWhenImportingTsRollupAndRetentionWithCommunityLicense()
        {
            DoNotReuseServer();

            var file = GetTempFileName();
            try
            {
                using (var store = GetDocumentStore())
                {
                    LicenseHelper.CreateTsRollupAndRetention(store);

                    var operation = await store.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
                }

                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.ChangeLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_COMM);

                    var exception = await Assert.ThrowsAsync<LicenseLimitException>(async () =>
                    {
                        var importOperation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                        await importOperation.WaitForCompletionAsync(TimeSpan.FromMinutes(5));
                    });
                    Assert.Equal(LimitType.TimeSeriesRollupsAndRetention, exception.LimitType);
                    Assert.True(exception.Message.Contains("Your license doesn't support adding Time Series Rollups And Retention feature."));
                }
            }
            finally
            {
                File.Delete(file);
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Compression)]
        public async Task ExceptionWhenImportingCompressionWithCommunityLicense()
        {
            DoNotReuseServer();
            var file = GetTempFileName();
            try
            {
                using (var store = GetDocumentStore())
                {
                    LicenseHelper.CreateCompressAllCollection(store);

                    var operation = await store.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
                }

                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.ChangeLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_COMM);

                    var exception = await Assert.ThrowsAsync<LicenseLimitException>(async () =>
                    {
                        var importOperation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                        await importOperation.WaitForCompletionAsync(TimeSpan.FromMinutes(5));
                    });
                    Assert.Equal(LimitType.DocumentsCompression, exception.LimitType);
                    Assert.True(exception.Message.Contains("Your license doesn't support adding Documents Compression feature."));
                }
            }
            finally
            {
                File.Delete(file);
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Compression)]
        public async Task ExceptionWhenImportingCompressionWithProLicense()
        {
            DoNotReuseServer();
            var file = GetTempFileName();
            try
            {
                using (var store = GetDocumentStore())
                {
                    LicenseHelper.CreateCompressAllCollection(store);

                    var operation = await store.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
                }

                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.ChangeLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_PRO);

                    var exception = await Assert.ThrowsAsync<LicenseLimitException>(async () =>
                    {
                        var importOperation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                        await importOperation.WaitForCompletionAsync(TimeSpan.FromMinutes(5));
                    });
                    Assert.Equal(LimitType.DocumentsCompression, exception.LimitType);
                    Assert.True(exception.Message.Contains("Your license doesn't support adding Documents Compression feature."));
                }
            }
            finally
            {
                File.Delete(file);
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Replication)]
        public async Task ExceptionWhenImportingPullReplicationAsSinkWithCommunityLicense()
        {
            DoNotReuseServer();

            var file = GetTempFileName();
            var dbName = $"db/{Guid.NewGuid()}";
            var csName = $"cs/{Guid.NewGuid()}";
            try
            {
                PullReplicationAsSink pullAsSink;
                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.DisableRevisionCompression(Server, store);

                    pullAsSink = await LicenseHelper.CreatePullReplicationAsSink(dbName, csName, store);

                    var operation = await store.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
                }

                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.ChangeLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_COMM);
                    var importOperation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                    await importOperation.WaitForCompletionAsync(TimeSpan.FromMinutes(5));

                    var exception = await Assert.ThrowsAsync<LicenseLimitException>(async () =>
                    {
                        pullAsSink.Disabled = false;
                        await store.Maintenance.SendAsync(new UpdatePullReplicationAsSinkOperation(pullAsSink));
                    });
                    Assert.Equal(LimitType.PullReplicationAsSink, exception.LimitType);
                }
            }
            finally
            {
                File.Delete(file);
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Replication)]
        public async Task ExceptionWhenImportingPullReplicationAsHubWithCommunityLicense()
        {
            DoNotReuseServer();

            var file = GetTempFileName();
            try
            {
                PullReplicationDefinition pull;
                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.DisableRevisionCompression(Server, store);
                    pull = LicenseHelper.CraetePullReplicationDefinition(store);

                    var operation = await store.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
                }

                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.ChangeLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_COMM);

                    var importOperation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                    await importOperation.WaitForCompletionAsync(TimeSpan.FromMinutes(5));
                    pull.Disabled = false;
                    var exception = Assert.Throws<LicenseLimitException>(() =>
                    {
                        store.Maintenance.Send(new PutPullReplicationAsHubOperation(pull));
                    });
                    Assert.Equal(LimitType.PullReplicationAsHub, exception.LimitType);
                }
            }
            finally
            {
                File.Delete(file);
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Replication)]
        public async Task ExceptionWhenImportingPullReplicationAsHubWithProLicense()
        {
            DoNotReuseServer();

            var file = GetTempFileName();
            try
            {
                PullReplicationDefinition pull;
                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.DisableRevisionCompression(Server, store);
                    pull = LicenseHelper.CraetePullReplicationDefinition(store);

                    var operation = await store.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
                }

                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.ChangeLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_PRO);

                    var importOperation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                    await importOperation.WaitForCompletionAsync(TimeSpan.FromMinutes(5));
                    pull.Disabled = false;
                    var exception = Assert.Throws<LicenseLimitException>(() =>
                    {
                        store.Maintenance.Send(new PutPullReplicationAsHubOperation(pull));
                    });
                    Assert.Equal(LimitType.PullReplicationAsHub, exception.LimitType);
                }
            }
            finally
            {
                File.Delete(file);
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Etl)]
        public async Task ExceptionWhenImportingRavenEtlWithCommunityLicense()
        {
            DoNotReuseServer();
            var dbName = $"db/{Guid.NewGuid()}";
            var csName = $"cs/{Guid.NewGuid()}";
            var file = GetTempFileName();
            try
            {
                AddEtlOperationResult etl;
                RavenEtlConfiguration etlConfiguration;
                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.DisableRevisionCompression(Server, store);
                    etlConfiguration = LicenseHelper.CreateRavenEtlConfiguration(csName, dbName, store, out etl);

                    var operation = await store.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
                }

                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.ChangeLicenseAndDisableRevisionCompression(Server, store,LicenseTestBase.RL_COMM);

                    var importOperation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                    await importOperation.WaitForCompletionAsync(TimeSpan.FromMinutes(5));
                    etlConfiguration.Disabled = false;
                    var op = new UpdateEtlOperation<RavenConnectionString>(etl.TaskId, etlConfiguration);

                    var exception = Assert.Throws<LicenseLimitException>( () =>
                    {
                        store.Maintenance.Send(op);
                    });
                    Assert.Equal(LimitType.RavenEtl, exception.LimitType);
                }
            }
            finally
            {
                File.Delete(file);
            }
        }
    }
}
