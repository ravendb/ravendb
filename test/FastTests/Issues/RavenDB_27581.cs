using System;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Raven.Client.Documents.Operations.Backups;
using Raven.Server.Documents.PeriodicBackup;
using Sparrow.Platform;
using Tests.Infrastructure;
using Xunit;
using ITestOutputHelper = Xunit.ITestOutputHelper;

namespace FastTests.Issues
{
    public class RavenDB_27581 : RavenTestBase
    {
        private const string RemoteFolderFromScript = "account/product";

        public RavenDB_27581(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.BackupExportImport)]
        [InlineData("S3")]
        [InlineData("Azure")]
        public async Task ServerWideDirectUploadWithConfigurationScriptShouldAddDatabaseNameToRemoteFolder(string destination)
        {
            using var store = GetDocumentStore();
            var database = await GetDatabase(store.Database);

            var configuration = new PeriodicBackupConfiguration();
            string scriptPath;

            if (destination == "S3")
            {
                scriptPath = GenerateConfigurationScript(new S3Settings { BucketName = "bucket", AwsRegionName = "us-east-1", RemoteFolderName = RemoteFolderFromScript }, out string command);
                configuration.S3Settings = new S3Settings { GetBackupConfigurationScript = new GetBackupConfigurationScript { Exec = command, Arguments = scriptPath } };
            }
            else
            {
                scriptPath = GenerateConfigurationScript(new AzureSettings { StorageContainer = "container", AccountName = "account", RemoteFolderName = RemoteFolderFromScript }, out string command);
                configuration.AzureSettings = new AzureSettings { GetBackupConfigurationScript = new GetBackupConfigurationScript { Exec = command, Arguments = scriptPath } };
            }

            try
            {
                var uploaderSettings = UploaderSettings.GenerateUploaderSettingsForBackup(database, configuration, "Server Wide Backup, test", isServerWide: true,
                    backupToLocalFolder: false, registerOnBackupException: null);

                var expectedRemoteFolder = $"{RemoteFolderFromScript}/{store.Database}";

                if (destination == "S3")
                {
                    Assert.Equal(expectedRemoteFolder, uploaderSettings.S3Settings.RemoteFolderName);
                    Assert.Null(configuration.S3Settings.RemoteFolderName);
                }
                else
                {
                    Assert.Equal(expectedRemoteFolder, uploaderSettings.AzureSettings.RemoteFolderName);
                    Assert.Null(configuration.AzureSettings.RemoteFolderName);
                }
            }
            finally
            {
                File.Delete(scriptPath);
            }
        }

        private static string GenerateConfigurationScript(BackupSettings settings, out string command)
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
