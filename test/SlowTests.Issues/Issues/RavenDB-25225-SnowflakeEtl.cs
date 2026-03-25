using System;
using System.IO;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.Snowflake;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Exceptions.Commercial;
using Raven.Client.Util;
using Raven.Server.ServerWide.Commands.ETL;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_25225_SnowflakeEtl : RavenTestBase
    {
        // ----------------------------------------
        // Tests for Snowflake Etl License Limits
        //
        // When we import, Snowflake is disabled by default.
        // ----------------------------------------

        public RavenDB_25225_SnowflakeEtl(ITestOutputHelper output) : base(output)
        {
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Etl)]
        public async Task PreventLicenseDowngradeSnowflakeEtl()
        {
            DoNotReuseServer();
            using (var store = GetDocumentStore())
            {
                await LicenseHelper.CreateSnowflakeEtlConfiguration(store);

                await LicenseHelper.FailToChangeLicense(Server, LicenseTestBase.RL_COMM, LimitType.SnowflakeEtl);
                await LicenseHelper.FailToChangeLicense(Server, LicenseTestBase.RL_PRO, LimitType.SnowflakeEtl);
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Etl)]
        public async Task PreventPutSnowflakeEtlWithCommunityLicense()
        {
            DoNotReuseServer();
            using (var store = GetDocumentStore())
            {
                await LicenseHelper.PutLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_COMM);

                var exception = await Assert.ThrowsAsync<LicenseLimitException>(async () =>
                {
                    await LicenseHelper.CreateSnowflakeEtlConfiguration(store);
                });
                Assert.Equal(LimitType.SnowflakeEtl, exception.LimitType);

                var config = LicenseHelper.GetSnowflakeEtlConfiguration(false, LicenseHelper.GetSnowflakeConnectionString());
                exception = await Assert.ThrowsAsync<LicenseLimitException>(async () =>
                {
                    await Server.ServerStore.SendToLeaderAsync(new AddSnowflakeEtlCommand(config, store.Database, RaftIdGenerator.NewId()));
                });
                Assert.Equal(LimitType.SnowflakeEtl, exception.LimitType);
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Etl)]
        public async Task PutDisabledSnowflakeEtlWithCommunityLicense()
        {
            DoNotReuseServer();
            using (var store = GetDocumentStore())
            {
                await LicenseHelper.PutLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_COMM);

                await LicenseHelper.CreateSnowflakeEtlConfiguration(store, true);
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Etl | RavenTestCategory.BackupExportImport)]
        public async Task ImportingDisabledSnowflakeEtlWithCommunityLicense()
        {
            DoNotReuseServer();
            var file = GetTempFileName();
            try
            {
                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.DisableRevisionCompression(Server, store);

                    await LicenseHelper.CreateSnowflakeEtlConfiguration(store, true);

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

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Etl | RavenTestCategory.BackupExportImport)]
        public async Task RestoreDisabledSnowflakeEtlWithCommunityLicense()
        {
            DoNotReuseServer();
            var backupPath = NewDataPath(suffix: "BackupFolder");
            try
            {
                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.DisableRevisionCompression(Server, store);

                    await LicenseHelper.CreateSnowflakeEtlConfiguration(store, true);

                    await LicenseHelper.RunBackup(store, backupPath);
                }

                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.PutLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_COMM);

                    LicenseHelper.RunRestore(store, backupPath);
                }
            }
            finally
            {
                Directory.Delete(backupPath, true);
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Etl | RavenTestCategory.BackupExportImport)]
        public async Task RestoreSnowflakeEtlWithCommunityLicense()
        {
            DoNotReuseServer();
            var backupPath = NewDataPath(suffix: "BackupFolder");
            try
            {
                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.DisableRevisionCompression(Server, store);

                    await LicenseHelper.CreateSnowflakeEtlConfiguration(store);
                    await LicenseHelper.RunBackup(store, backupPath);
                }

                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.PutLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_COMM);

                    var exception = Assert.Throws<LicenseLimitException>(() =>
                    {
                        LicenseHelper.RunRestore(store, backupPath);
                    });
                    Assert.Equal(LimitType.SnowflakeEtl, exception.LimitType);
                }
            }
            finally
            {
                Directory.Delete(backupPath, true);
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Ai)]
        public async Task EnableSnowflakeEtlWithCommunityLicense()
        {
            DoNotReuseServer();
            using (var store = GetDocumentStore())
            {
                await LicenseHelper.PutLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_COMM);
                var config = await LicenseHelper.CreateSnowflakeEtlConfiguration(store, true);
                var op = new GetOngoingTaskInfoOperation(config.Name, OngoingTaskType.SnowflakeEtl);

                var res = store.Maintenance.Send(op);

                var exception = await Assert.ThrowsAsync<LicenseLimitException>(async () =>
                {
                    config.Disabled = false;

                    await store.Maintenance.SendAsync(new UpdateEtlOperation<SnowflakeConnectionString>(res.TaskId, config));
                });
                Assert.Equal(LimitType.SnowflakeEtl, exception.LimitType);

                exception = await Assert.ThrowsAsync<LicenseLimitException>(async () =>
                {
                    config.Disabled = false;
                    await Server.ServerStore.SendToLeaderAsync(new UpdateSnowflakeEtlCommand(res.TaskId, config, store.Database, RaftIdGenerator.NewId()));
                });
                Assert.Equal(LimitType.SnowflakeEtl, exception.LimitType);
            }
        }
    }
}
