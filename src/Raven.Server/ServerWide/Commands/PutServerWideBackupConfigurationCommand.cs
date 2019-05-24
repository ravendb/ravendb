using System.IO;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.ServerWide.Operations.Configuration;
using Raven.Server.ServerWide.Context;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public class PutServerWideBackupConfigurationCommand : PutValueCommand<ServerWideBackupConfiguration>
    {
        public PutServerWideBackupConfigurationCommand()
        {
            // for deserialization
        }

        public PutServerWideBackupConfigurationCommand(ServerWideBackupConfiguration configuration)
        {
            Name = ClusterStateMachine.BackupTemplateConfigurationName;
            Value = configuration;
        }

        public override DynamicJsonValue ValueToJson()
        {
            return Value?.ToJson();
        }

        public override void VerifyCanExecuteCommand(ServerStore store, TransactionOperationContext context, bool isClusterAdmin)
        {
            AssertClusterAdmin(isClusterAdmin);
        }

        public static void UpdateTemplateForDatabase(PeriodicBackupConfiguration configuration, string databaseName)
        {
            var localSettings = configuration.LocalSettings;
            if (localSettings?.FolderPath != null)
            {
                localSettings.FolderPath = GetUpdatedPath(localSettings.FolderPath, Path.DirectorySeparatorChar);
            }

            var s3Settings = configuration.S3Settings;
            if (s3Settings != null)
            {
                s3Settings.RemoteFolderName = GetUpdatedPath(s3Settings.RemoteFolderName);
            }

            var azureSettings = configuration.AzureSettings;
            if (azureSettings != null)
            {
                azureSettings.RemoteFolderName = GetUpdatedPath(azureSettings.RemoteFolderName);
            }

            var googleCloudSettings = configuration.GoogleCloudSettings;
            if (googleCloudSettings != null)
            {
                googleCloudSettings.RemoteFolderName = GetUpdatedPath(googleCloudSettings.RemoteFolderName);
            }

            var ftpSettings = configuration.FtpSettings;
            if (ftpSettings?.Url != null)
            {
                ftpSettings.Url = GetUpdatedPath(ftpSettings.Url);
            }

            string GetUpdatedPath(string str, char separator = '/')
            {
                if (str == null)
                    return databaseName;

                if (str.EndsWith(separator) == false)
                    str += separator;

                return str + databaseName;
            }
        }
    }
}
