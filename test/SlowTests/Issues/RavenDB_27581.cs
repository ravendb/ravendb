using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Operations.Configuration;
using Raven.Server.Documents.PeriodicBackup.Aws;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Platform;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_27581 : RestoreFromS3TestBase
    {
        public RavenDB_27581(ITestOutputHelper output) : base(output)
        {
            DoNotReuseServer();
        }

        [AmazonS3RetryFact, Trait("Category", "BackupExportImport")]
        public async Task ServerWideDirectUploadWithConfigurationScriptShouldWriteEachDatabaseToItsOwnFolder()
        {
            var s3Settings = GetS3Settings();
            var scriptPath = GenerateConfigurationScript(s3Settings, out string command);

            try
            {
                using var store1 = GetDocumentStore();
                using var store2 = GetDocumentStore();

                await StoreUser(store1);
                await StoreUser(store2);

                var serverWideConfiguration = new ServerWideBackupConfiguration
                {
                    FullBackupFrequency = "0 2 * * 0",
                    BackupUploadMode = BackupUploadMode.DirectUpload,
                    S3Settings = new S3Settings
                    {
                        GetBackupConfigurationScript = new GetBackupConfigurationScript { Exec = command, Arguments = scriptPath }
                    }
                };

                await store1.Maintenance.Server.SendAsync(new PutServerWideBackupConfigurationOperation(serverWideConfiguration));

                await RunServerWideBackup(store1);
                await RunServerWideBackup(store2);

                using (var cts = new CancellationTokenSource(TimeSpan.FromMinutes(1)))
                using (var s3Client = new RavenAwsS3Client(s3Settings, DefaultConfiguration, cancellationToken: cts.Token))
                {
                    var objects = await s3Client.ListObjectsAsync($"{s3Settings.RemoteFolderName}/", string.Empty, listFolders: false);
                    var keys = objects.FileInfoDetails.Select(x => x.FullPath).ToList();

                    var database1Prefix = $"{s3Settings.RemoteFolderName}/{store1.Database}/";
                    var database2Prefix = $"{s3Settings.RemoteFolderName}/{store2.Database}/";

                    Assert.Contains(keys, key => key.StartsWith(database1Prefix, StringComparison.Ordinal));
                    Assert.Contains(keys, key => key.StartsWith(database2Prefix, StringComparison.Ordinal));
                    Assert.All(keys, key => Assert.True(key.StartsWith(database1Prefix, StringComparison.Ordinal) || key.StartsWith(database2Prefix, StringComparison.Ordinal),
                        $"Backup key '{key}' is not under a per-database folder"));
                }
            }
            finally
            {
                File.Delete(scriptPath);
            }
        }

        private static async Task StoreUser(IDocumentStore store)
        {
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = "Efrat" }, "users/1");
                await session.SaveChangesAsync();
            }
        }

        private async Task RunServerWideBackup(IDocumentStore store)
        {
            var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
            var backupTask = record.PeriodicBackups.Single();

            Assert.Equal(BackupUploadMode.DirectUpload, backupTask.BackupUploadMode);

            await Backup.RunBackupAsync(Server, backupTask.TaskId, store);
        }

        private static string GenerateConfigurationScript(S3Settings settings, out string command)
        {
            var scriptPath = Path.Combine(Path.GetTempPath(), Path.ChangeExtension(Guid.NewGuid().ToString(), ".ps1"));
            var settingsString = JsonConvert.SerializeObject(settings);

            if (PlatformDetails.RunningOnPosix)
            {
                command = "bash";
                File.WriteAllText(scriptPath, $"#!/bin/bash\r\necho '{settingsString}'");
                Process.Start("chmod", $"700 {scriptPath}")?.WaitForExit();
            }
            else
            {
                command = "powershell";
                File.WriteAllText(scriptPath, $"echo '{settingsString}'");
            }

            return scriptPath;
        }
    }
}
