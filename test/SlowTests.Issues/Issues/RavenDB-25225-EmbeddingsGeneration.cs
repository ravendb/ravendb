using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Exceptions.Commercial;
using Raven.Client.Util;
using Raven.Server.ServerWide.Commands.AI;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_25225_EmbeddingsGeneration(ITestOutputHelper output) : EmbeddingsGenerationTestBase(output)
    {
        // ----------------------------------------
        // Tests for EmbeddingsGeneration License Limits
        //
        // When we import, Embeddings Generation is disabled by default.
        // ----------------------------------------

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Ai)]
        public async Task Prevent_License_Downgrade_Embeddings_Generation()
        {
            DoNotReuseServer();
            using (var store = GetDocumentStore())
            {
                await LicenseHelper.AddAiIntegration(store, Server);

                await LicenseHelper.FailToChangeLicense(Server, LicenseTestBase.RL_COMM, LimitType.EmbeddingsGeneration);
                await LicenseHelper.FailToChangeLicense(Server, LicenseTestBase.RL_PRO, LimitType.EmbeddingsGeneration);
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Ai)]
        public async Task PreventPutEmbeddingsGenerationWithCommunityLicense()
        {
            DoNotReuseServer();
            using (var store = GetDocumentStore())
            {
                await LicenseHelper.PutLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_COMM);

                var exception = await Assert.ThrowsAsync<LicenseLimitException>(async () =>
                {
                    await LicenseHelper.AddAiIntegration(store, Server);
                });
                Assert.Equal(LimitType.EmbeddingsGeneration, exception.LimitType);

            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Ai)]
        public async Task PutDisabledEmbeddingsGenerationWithCommunityLicense()
        {
            DoNotReuseServer();
            using (var store = GetDocumentStore())
            {
                await LicenseHelper.PutLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_COMM);

                await LicenseHelper.AddAiIntegration(store, Server, true);
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Ai | RavenTestCategory.BackupExportImport)]
        public async Task ImportingDisabledEmbeddingsGenerationWithCommunityLicense()
        {
            DoNotReuseServer();
            var file = GetTempFileName();
            try
            {
                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.DisableRevisionCompression(Server, store);

                    await LicenseHelper.AddAiIntegration(store, Server, true);

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
        public async Task ImportingDisabledEmbeddingsGenerationWithProLicense()
        {
            DoNotReuseServer();
            var file = GetTempFileName();
            try
            {
                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.DisableRevisionCompression(Server, store);

                    await LicenseHelper.AddAiIntegration(store, Server, true);

                    var operation = await store.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
                }

                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.PutLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_PRO);

                    var importOperation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                    await importOperation.WaitForCompletionAsync(TimeSpan.FromMinutes(5));

                    var operation = new GetOngoingTaskInfoOperation(DefaultEmbeddingGenerationTaskName, OngoingTaskType.EmbeddingsGeneration);
                    var res = store.Maintenance.Send(operation);

                    var exception = await Assert.ThrowsAsync<LicenseLimitException>(async () =>
                    {
                        var command = new UpdateEmbeddingsGenerationCommand(res.TaskId, LicenseHelper.EGConfig(false, LicenseHelper.EgConnectionString()), store.Database, RaftIdGenerator.NewId());
                        await Server.ServerStore.SendToLeaderAsync(command);
                    });
                    Assert.Equal(LimitType.EmbeddingsGeneration, exception.LimitType);
                }
            }
            finally
            {
                File.Delete(file);
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Ai | RavenTestCategory.BackupExportImport)]
        public async Task RestoreDisabledEmbeddingsGenerationWithCommunityLicense()
        {
            DoNotReuseServer();
            var backupPath = NewDataPath(suffix: "BackupFolder");
            try
            {
                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.DisableRevisionCompression(Server, store);

                    await LicenseHelper.AddAiIntegration(store, Server, true);

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

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Ai | RavenTestCategory.BackupExportImport)]
        public async Task RestoreEmbeddingsGenerationWithCommunityLicense()
        {
            DoNotReuseServer();
            var backupPath = NewDataPath(suffix: "BackupFolder");
            try
            {
                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.DisableRevisionCompression(Server, store);

                    await LicenseHelper.AddAiIntegration(store, Server);

                    await LicenseHelper.RunBackup(store, backupPath);
                }

                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.PutLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_COMM);

                    var exception = Assert.Throws<LicenseLimitException>(() =>
                    {
                        LicenseHelper.RunRestore(store, backupPath);
                    });
                    Assert.Equal(LimitType.EmbeddingsGeneration, exception.LimitType);
                }
            }
            finally
            {
                Directory.Delete(backupPath, true);
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Ai)]
        public async Task EnableEmbeddingsGenerationWithCommunityLicense()
        {
            DoNotReuseServer();
            using (var store = GetDocumentStore())
            {
                await LicenseHelper.PutLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_COMM);

                var config = await LicenseHelper.AddAiIntegration(store, Server, true);

                var exception = await Assert.ThrowsAsync<LicenseLimitException>(async () =>
                {
                    await LicenseHelper.UpdateAiIntegration(store, Server, config);
                });
                Assert.Equal(LimitType.EmbeddingsGeneration, exception.LimitType);
            }
        }
            }
        }
