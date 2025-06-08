using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Newtonsoft.Json;
using Raven.Client;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Smuggler;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Documents.AI.GenAi;
using Raven.Server.Documents.ETL;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.AI.GenAi;

public class GenAiBackupRestore(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenTheory(RavenTestCategory.Etl | RavenTestCategory.Ai | RavenTestCategory.Smuggler)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.Ollama, DatabaseMode = RavenDatabaseMode.Single, CheckCanConnect = false, NightlyBuildRequired = false)]
    public async Task CanExportAndImportGenAiConfiguration(Options options, GenAiConfiguration config)
    {
        var exportFile = GetTempFileName();

        using (var src = GetDocumentStore(options))
        using (var dst = GetDocumentStore(options))
        {
            config.Prompt = "Translate the following sentence";
            config.Collection = "Posts";
            config.JsonSchema = OllamaChatCompletionClient.GetSchemaFor(JsonConvert.SerializeObject(new { Translation = "foo" }));
            config.UpdateScript = "this.Translation = $output.Translation";
            config.GenAiTransformation = new GenAiTransformation { Script = "ai.genContext({ Sentence: this.Body });" };
            src.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            src.Maintenance.Send(new AddGenAiOperation(config));

            await (await src.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), exportFile)).WaitForCompletionAsync();
            await (await dst.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), exportFile)).WaitForCompletionAsync();

            var dstRecord = await dst.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(dst.Database));
            Assert.Equal(1, dstRecord.GenAis.Count);
            Assert.Equal(1, dstRecord.AiConnectionStrings.Count);

            var imported = dstRecord.GenAis.First();
            Assert.Equal(config.Name, imported.Name);
            Assert.Equal(config.ConnectionStringName, imported.ConnectionStringName);
            Assert.Equal(config.Prompt, imported.Prompt);
            Assert.Equal(config.JsonSchema, imported.JsonSchema);
            Assert.Equal(config.UpdateScript, imported.UpdateScript);
            Assert.Equal(config.Collection, imported.Collection);
            Assert.Equal(config.GenAiTransformation.Script, imported.GenAiTransformation.Script);
        }
    }

    [RavenTheory(RavenTestCategory.Etl | RavenTestCategory.Ai | RavenTestCategory.BackupExportImport)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.Ollama, DatabaseMode = RavenDatabaseMode.Single, CheckCanConnect = false, NightlyBuildRequired = false, Data = new object[] { BackupType.Backup }, Skip = "flaky")]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.Ollama, DatabaseMode = RavenDatabaseMode.Single, CheckCanConnect = false, NightlyBuildRequired = false, Data = new object[] { BackupType.Snapshot }, Skip = "flaky")]
    public async Task CanBackupAndRestoreGenAiEtl(Options options, GenAiConfiguration config, BackupType backupType)
    {
        var backupPath = NewDataPath();
        var jsonSchema = OllamaChatCompletionClient.GetSchemaFor(JsonConvert.SerializeObject(new { Answer = "42" }));

        using (var store = GetDocumentStore())
        {
            config.Prompt = "Give a short answer to the following question";
            config.Collection = "Posts";
            config.JsonSchema = jsonSchema;
            config.UpdateScript = "this.GenAnswer = $output.Answer";
            config.GenAiTransformation = new GenAiTransformation { Script = "ai.genContext({ Question: this.Body });" };
            store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(config.Connection));
            store.Maintenance.Send(new AddGenAiOperation(config));

            var etlDone = Etl.WaitForEtlToComplete(store);

            // Add a doc
            const string id = "posts/1";
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new GenAiBasics.Post([new GenAiBasics.Comment("42", "Douglas Adams")], "So long, and Thanks", "What is the answer to life?"), id);
                await session.SaveChangesAsync();
            }

            Assert.True(etlDone.Wait(TimeSpan.FromSeconds(30)));

            string srcHash;
            using (var session = store.OpenSession())
            {
                var doc = session.Load<BlittableJsonReaderObject>(id);
                Assert.True(doc.TryGet("GenAnswer", out string genAnswer));
                Assert.NotNull(genAnswer);

                Assert.True(doc.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata));
                Assert.True(metadata.TryGet(Constants.Documents.Metadata.GenAiHashes, out BlittableJsonReaderObject hashesSection));
                Assert.Equal(1, hashesSection.Count);

                Assert.True(hashesSection.TryGet(config.Name, out BlittableJsonReaderArray hashes));
                Assert.Equal(1, hashes.Length);

                srcHash = hashes.Single().ToString();
            }

            var srcDb = await GetDatabase(store.Database);

            var srcState = EtlProcess.GetProcessState(srcDb, config.Name, config.Transforms[0].Name);
            var srcLastProcessedEtag = srcState.GetLastProcessedEtag(srcDb.DbBase64Id, Server.ServerStore.NodeTag);
            Assert.True(srcLastProcessedEtag > 0);

            // Perform backup
            var backupOp = await store.Maintenance.SendAsync(new BackupOperation(new BackupConfiguration
            {
                BackupType = backupType,
                LocalSettings = new LocalSettings
                {
                    FolderPath = backupPath
                }
            }));

            var result = (BackupResult)await backupOp.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
            var backupDir = Path.Combine(backupPath, result.LocalBackup.BackupDirectory);

            var restoredDb = $"{store}_Restore";

            using (Backup.RestoreDatabase(store, new RestoreBackupConfiguration
            {
                BackupLocation = backupDir,
                DatabaseName = restoredDb
            }))
            {
                var src = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                var dst = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(restoredDb));

                Assert.Equal(src.AiConnectionStrings.Count, dst.AiConnectionStrings.Count);
                Assert.Equal(src.GenAis.Count, dst.GenAis.Count);

                var srcGenConfig = src.GenAis.First();
                var dstGenConfig = dst.GenAis.First();

                Assert.Equal(srcGenConfig.Name, dstGenConfig.Name);
                Assert.Equal(srcGenConfig.ConnectionStringName, dstGenConfig.ConnectionStringName);
                Assert.Equal(srcGenConfig.Prompt, dstGenConfig.Prompt);
                Assert.Equal(srcGenConfig.JsonSchema, dstGenConfig.JsonSchema);
                Assert.Equal(srcGenConfig.UpdateScript, dstGenConfig.UpdateScript);
                Assert.Equal(srcGenConfig.Collection, dstGenConfig.Collection);
                Assert.Equal(srcGenConfig.GenAiTransformation.Script, dstGenConfig.GenAiTransformation.Script);

                var dstDb = await GetDatabase(restoredDb);

                var value = await WaitForValueAsync(() =>
                {
                    var dstState = EtlProcess.GetProcessState(dstDb, config.Name, config.Transforms[0].Name);
                    var lastProcessedEtag = dstState.GetLastProcessedEtag(dstDb.DbBase64Id, Server.ServerStore.NodeTag);
                    return Task.FromResult(lastProcessedEtag > 0);
                }, true, timeout: 60_000);

                Assert.True(value);

                using (var session = store.OpenSession(restoredDb))
                {
                    var doc = session.Load<BlittableJsonReaderObject>(id);
                    Assert.NotNull(doc);

                    Assert.True(doc.TryGet("GenAnswer", out string genAnswer));
                    Assert.NotNull(genAnswer);

                    Assert.True(doc.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata));
                    Assert.True(metadata.TryGet(Constants.Documents.Metadata.GenAiHashes, out BlittableJsonReaderObject hashesSection));
                    Assert.Equal(1, hashesSection.Count);

                    Assert.True(hashesSection.TryGet(config.Name, out BlittableJsonReaderArray hashes));
                    Assert.Equal(1, hashes.Length);

                    var dstHash = hashes.Single().ToString();

                    Assert.Equal(srcHash, dstHash);
                }

            }
        }
    }

    [RavenTheory(RavenTestCategory.Sharding | RavenTestCategory.Etl | RavenTestCategory.Ai | RavenTestCategory.BackupExportImport)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.Ollama, DatabaseMode = RavenDatabaseMode.Sharded, CheckCanConnect = false, NightlyBuildRequired = false)]
    public async Task CanBackupAndRestoreGenAiInShardedDatabase(Options options, GenAiConfiguration config)
    {
        var backupPath = NewDataPath(suffix: "BackupFolder");

        using (var store = GetDocumentStore(options))
        {
            store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            config.Prompt = "What is the answer to life?";
            config.Collection = "Posts";
            config.JsonSchema = OllamaChatCompletionClient.GetSchemaFor(JsonConvert.SerializeObject(new { Answer = "42" }));
            config.UpdateScript = "this.GenAnswer = $output.Answer";
            config.GenAiTransformation = new GenAiTransformation { Script = "ai.genContext({ Question: this.Body });" };

            await store.Maintenance.SendAsync(new AddGenAiOperation(config));

            var operation = await store.Maintenance.SendAsync(new BackupOperation(new BackupConfiguration
            {
                BackupType = BackupType.Backup,
                LocalSettings = new LocalSettings
                {
                    FolderPath = backupPath
                }
            }));

            await operation.WaitForCompletionAsync(TimeSpan.FromSeconds(60));

            var backupDirs = Directory.GetDirectories(backupPath);
            var sharding = await Sharding.GetShardingConfigurationAsync(store);
            var settings = Sharding.Backup.GenerateShardRestoreSettings(backupDirs, sharding);

            var restoreName = $"{store}_Restored";

            using (Backup.RestoreDatabase(store, new RestoreBackupConfiguration
            {
                DatabaseName = restoreName,
                ShardRestoreSettings = settings
            }))
            {
                var restored = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(restoreName));
                Assert.Equal(1, restored.GenAis.Count);
                Assert.Equal(1, restored.AiConnectionStrings.Count);

                var restoredGenConfig = restored.GenAis.First();

                Assert.Equal(config.Name, restoredGenConfig.Name);
                Assert.Equal(config.ConnectionStringName, restoredGenConfig.ConnectionStringName);
                Assert.Equal(config.Prompt, restoredGenConfig.Prompt);
                Assert.Equal(config.JsonSchema, restoredGenConfig.JsonSchema);
                Assert.Equal(config.UpdateScript, restoredGenConfig.UpdateScript);
                Assert.Equal(config.Collection, restoredGenConfig.Collection);
                Assert.Equal(config.GenAiTransformation.Script, restoredGenConfig.GenAiTransformation.Script);
            }
        }
    }

}
