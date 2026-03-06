using System;
using System.IO;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Exceptions.Commercial;
using Raven.Client.Util;
using Raven.Server.ServerWide.Commands.AI;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_25225_GenAi(ITestOutputHelper output) : RavenTestBase(output)
    {
        // ----------------------------------------
        // Tests for GenAI License Limits
        //
        // When we import, GenAI is disabled by default.
        // ----------------------------------------

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Ai)]
        public async Task PreventLicenseDowngradeGenAi()
        {
            DoNotReuseServer();
            using (var store = GetDocumentStore())
            {
                await LicenseHelper.AddGenAiIntegration(store);

                await LicenseHelper.FailToChangeLicense(Server, LicenseTestBase.RL_COMM, LimitType.GenAi);
                await LicenseHelper.FailToChangeLicense(Server, LicenseTestBase.RL_PRO, LimitType.GenAi);
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Ai)]
        public async Task FailToPutGenAiWithCommunityLicense()
        {
            DoNotReuseServer();
            using (var store = GetDocumentStore())
            {
                await LicenseHelper.PutLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_COMM);

                var exception = await Assert.ThrowsAsync<LicenseLimitException>(async () =>
                {
                    await LicenseHelper.AddGenAiIntegration(store);
                });
                Assert.Equal(LimitType.GenAi, exception.LimitType);
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Ai)]
        public async Task PutDisabledGenAiWithCommunityLicense()
        {
            DoNotReuseServer();
            using (var store = GetDocumentStore())
            {
                await LicenseHelper.PutLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_COMM);

                await LicenseHelper.AddGenAiIntegration(store, true);
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Ai | RavenTestCategory.BackupExportImport)]
        public async Task FailToRestoreGenAiWithCommunityLicense()
        {
            DoNotReuseServer();
            var backupPath = NewDataPath(suffix: "BackupFolder");
            try
            {
                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.DisableRevisionCompression(Server, store);
                    await LicenseHelper.AddGenAiIntegration(store);
                    await LicenseHelper.RunBackup(store, backupPath);
                }

                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.PutLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_COMM);

                    var exception = Assert.Throws<LicenseLimitException>(() =>
                    {
                        LicenseHelper.RunRestore(store, backupPath);
                    });
                    Assert.Equal(LimitType.GenAi, exception.LimitType);
                }
            }
            finally
            {
                Directory.Delete(backupPath, true);
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Ai | RavenTestCategory.BackupExportImport)]
        public async Task ImportingDisabledGenAiWithCommunityLicense()
        {
            DoNotReuseServer();
            var file = GetTempFileName();
            try
            {
                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.DisableRevisionCompression(Server, store);
                    await LicenseHelper.AddGenAiIntegration(store, true);

                    var operation = await store.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
                }

                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.PutLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_COMM);
                    var importOperation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                    await importOperation.WaitForCompletionAsync(TimeSpan.FromMinutes(5));

                    var operation = new GetOngoingTaskInfoOperation(LicenseHelper.DefaultGenAiTaskName, OngoingTaskType.GenAi);
                    var res = store.Maintenance.Send(operation);

                    var exception = await Assert.ThrowsAsync<LicenseLimitException>(async () =>
                    {
                        var command = new UpdateGenAiCommand(res.TaskId, LicenseHelper.GenAiConfig(false), store.Database, "", RaftIdGenerator.NewId());
                        await Server.ServerStore.SendToLeaderAsync(command);
                    });
                    Assert.Equal(LimitType.GenAi, exception.LimitType);
                }
            }
            finally
            {
                File.Delete(file);
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Ai | RavenTestCategory.BackupExportImport)]
        public async Task ImportingDisabledGenAiWithProLicense()
        {
            DoNotReuseServer();
            var file = GetTempFileName();
            try
            {
                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.DisableRevisionCompression(Server, store);
                    await LicenseHelper.AddGenAiIntegration(store, true);

                    var operation = await store.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
                }

                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.PutLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_PRO);

                    var importOperation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                    await importOperation.WaitForCompletionAsync(TimeSpan.FromMinutes(5));

                    var operation = new GetOngoingTaskInfoOperation(LicenseHelper.DefaultGenAiTaskName, OngoingTaskType.GenAi);
                    var res = store.Maintenance.Send(operation);

                    var exception = await Assert.ThrowsAsync<LicenseLimitException>(async () =>
                    {
                        var command = new UpdateGenAiCommand(res.TaskId, LicenseHelper.GenAiConfig(false), store.Database, "",RaftIdGenerator.NewId());
                        await Server.ServerStore.SendToLeaderAsync(command);
                    });
                    Assert.Equal(LimitType.GenAi, exception.LimitType);
                }
            }
            finally
            {
                File.Delete(file);
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Ai | RavenTestCategory.BackupExportImport)]
        public async Task RestoreDisabledGenAiWithCommunityLicense()
        {
            DoNotReuseServer();
            var backupPath = NewDataPath(suffix: "BackupFolder");
            try
            {
                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.DisableRevisionCompression(Server, store);
                    await LicenseHelper.AddGenAiIntegration(store, true);
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

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Ai)]
        public async Task FailedToEnableGenAiCommunityLicense()
        {
            DoNotReuseServer();
            var file = GetTempFileName();
            try
            {
                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.DisableRevisionCompression(Server, store);
                    await LicenseHelper.AddGenAiIntegration(store, true);

                    var operation = await store.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
                }

                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.RunImport(Server, store, file);

                    var operation = new GetOngoingTaskInfoOperation(LicenseHelper.DefaultGenAiTaskName, OngoingTaskType.GenAi);
                    var res = store.Maintenance.Send(operation);

                    var exception = await Assert.ThrowsAsync<LicenseLimitException>(async () =>
                    {
                        var command = new UpdateGenAiCommand(res.TaskId, LicenseHelper.GenAiConfig(false), store.Database, "", RaftIdGenerator.NewId());
                        await Server.ServerStore.SendToLeaderAsync(command);
                    });

                    Assert.Equal(LimitType.GenAi, exception.LimitType);
                }
            }
            finally
            {
                File.Delete(file);
            }
        }

    }
}
