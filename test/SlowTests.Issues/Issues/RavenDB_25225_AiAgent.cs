using System;
using System.IO;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Exceptions.Commercial;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_25225_AiAgent(ITestOutputHelper output) : RavenTestBase(output)
    {
        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Ai)]
        public async Task Prevent_License_Downgrade_AiAgent()
        {
            DoNotReuseServer();
            using (var store = GetDocumentStore())
            {
                await LicenseHelper.AddAiAgentIntegration(store);

                await LicenseHelper.FailToChangeLicense(Server, LicenseTestBase.RL_COMM, LimitType.AiAgent);
                await LicenseHelper.FailToChangeLicense(Server, LicenseTestBase.RL_PRO, LimitType.AiAgent);
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
                    await LicenseHelper.AddAiAgentIntegration(store);
                });
                Assert.Equal(LimitType.AiAgent, exception.LimitType);
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Ai)]
        public async Task PutDisabledGenAiWithCommunityLicense()
        {
            DoNotReuseServer();
            using (var store = GetDocumentStore())
            {
                await LicenseHelper.PutLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_COMM);

                await LicenseHelper.AddAiAgentIntegration(store, true);
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
                    await LicenseHelper.AddAiAgentIntegration(store);
                    await LicenseHelper.RunBackup(store, backupPath);
                }

                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.PutLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_COMM);

                    var exception = Assert.Throws<LicenseLimitException>(() =>
                    {
                        LicenseHelper.RunRestore(store, backupPath);
                    });
                    Assert.Equal(LimitType.AiAgent, exception.LimitType);
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
                    await LicenseHelper.AddAiAgentIntegration(store, true);

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
                    await LicenseHelper.AddAiAgentIntegration(store, true);
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
                    await LicenseHelper.AddAiAgentIntegration(store, true);

                    var operation = await store.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
                }

                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.RunImport(Server, store, file);

                    var exception = await Assert.ThrowsAsync<LicenseLimitException>(async () =>
                    {
                        await LicenseHelper.AddAiAgentIntegration(store);
                    });

                    Assert.Equal(LimitType.AiAgent, exception.LimitType);
                }
            }
            finally
            {
                File.Delete(file);
            }
        }
    }
}
