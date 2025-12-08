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
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_25225_EmbeddingsGeneration(ITestOutputHelper output) : EmbeddingsGenerationTestBase(output)
    {
        // ----------------------------------------
        // Tests for EmbeddingsGeneration License Limits
        // ----------------------------------------

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

                    await AddAiIntegration(store, true);

                    var operation = await store.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
                }

                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.PutLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_COMM);

                    var importOperation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                    await importOperation.WaitForCompletionAsync(TimeSpan.FromMinutes(5));

                    var operation = new GetOngoingTaskInfoOperation(DefaultEmbeddingGenerationTaskName, OngoingTaskType.EmbeddingsGeneration);
                    var res = store.Maintenance.Send(operation);

                    var exception = await Assert.ThrowsAsync<LicenseLimitException>(async () =>
                    {
                        var command = new UpdateEmbeddingsGenerationCommand(res.TaskId, EGConfig(false, EgConnectionString()), store.Database, RaftIdGenerator.NewId());
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
        public async Task ImportingDisabledEmbeddingsGenerationWithProLicense()
        {
            DoNotReuseServer();
            var file = GetTempFileName();
            try
            {
                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.DisableRevisionCompression(Server, store);

                    await AddAiIntegration(store, true);

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
                        var command = new UpdateEmbeddingsGenerationCommand(res.TaskId, EGConfig(false, EgConnectionString()), store.Database, RaftIdGenerator.NewId());
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

                    await AddAiIntegration(store, true);

                    await RunBackup(store, backupPath);
                }

                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.PutLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_COMM);

                    RunRestore(store, backupPath);
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

                    await AddAiIntegration(store);

                    await RunBackup(store, backupPath);
                }

                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.PutLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_COMM);

                    var exception = Assert.Throws<LicenseLimitException>(() =>
                    {
                        RunRestore(store, backupPath);
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
        public async Task PreventPutEmbeddingsGenerationWithCommunityLicense()
        {
            DoNotReuseServer();
            using (var store = GetDocumentStore())
            {
                await LicenseHelper.PutLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_COMM);

                var exception = await Assert.ThrowsAsync<LicenseLimitException>(async () =>
                {
                    await AddAiIntegration(store);
                });
                Assert.Equal(LimitType.EmbeddingsGeneration, exception.LimitType);

            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Ai)]
        public async Task Prevent_License_Downgrade_Embeddings_Generation()
        {
            DoNotReuseServer();
            using (var store = GetDocumentStore())
            {
                WaitForUserToContinueTheTest(store);
                await AddAiIntegration(store);

                await LicenseHelper.FailToChangeLicense(Server, LicenseTestBase.RL_COMM, LimitType.EmbeddingsGeneration);
                await LicenseHelper.FailToChangeLicense(Server, LicenseTestBase.RL_PRO, LimitType.EmbeddingsGeneration);
            }
        }

        private async Task AddAiIntegration(DocumentStore store, bool disabled = false)
        {
            AiConnectionString connectionString = EgConnectionString();

            connectionString.Identifier = connectionString.GenerateIdentifier();
            EmbeddingsGenerationConfiguration config = EGConfig(disabled, connectionString);

            var putResult = store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(connectionString));
            Assert.NotNull(putResult.RaftCommandIndex);

            var command = new AddEmbeddingsGenerationCommand(config, store.Database, RaftIdGenerator.NewId());
            await Server.ServerStore.SendToLeaderAsync(command);
        }

        private static AiConnectionString EgConnectionString()
        {
            var connectionString = new AiConnectionString
            {
                Name = DefaultConnectionStringName,
                OllamaSettings = new OllamaSettings
                {
                    Uri = "http://localhost:11434",
                    Model = "test-model"
                }
            };
            return connectionString;
        }

        private static EmbeddingsGenerationConfiguration EGConfig(bool disabled, AiConnectionString connectionString)
        {
            var config = new EmbeddingsGenerationConfiguration
            {
                Name = DefaultEmbeddingGenerationTaskName,
                ConnectionStringName = DefaultConnectionStringName,
                EmbeddingsPathConfigurations = [new EmbeddingPathConfiguration() { Path = "Name", ChunkingOptions = DefaultChunkingOptions }],
                Collection = "Dtos",
                ChunkingOptionsForQuerying = DefaultChunkingOptions,
                Disabled = disabled,
                Connection = connectionString
            };
            config.Identifier = config.GenerateIdentifier();
            return config;
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Ai)]
        public async Task PutDisabledEmbeddingsGenerationWithCommunityLicense()
        {
            DoNotReuseServer();
            using (var store = GetDocumentStore())
            {
                await LicenseHelper.PutLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_COMM);

                await AddAiIntegration(store, true);
            }
        }

        private void RunRestore(DocumentStore store, string backupPath)
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
