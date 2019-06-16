using System;
using System.IO;
using System.Linq;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.ServerWide.Operations.Configuration;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public class PutServerWideBackupConfigurationCommand : UpdateValueCommand<ServerWideBackupConfiguration>
    {
        protected PutServerWideBackupConfigurationCommand()
        {
            // for deserialization
        }

        public PutServerWideBackupConfigurationCommand(ServerWideBackupConfiguration configuration, string uniqueRequestId) : base(uniqueRequestId)
        {
            Name = ClusterStateMachine.ServerWideBackupConfigurationsKey;
            Value = configuration;
        }

        public override object ValueToJson()
        {
            return Value.ToJson();
        }

        public override BlittableJsonReaderObject GetUpdatedValue(JsonOperationContext context, BlittableJsonReaderObject previousValue, long index)
        {
            if (string.IsNullOrWhiteSpace(Value.Name))
            {
                Value.Name = GenerateTaskName(previousValue);
            }

            Value.TaskId = index;

            if (previousValue != null)
            {
                if (previousValue.Modifications == null)
                    previousValue.Modifications = new DynamicJsonValue();

                previousValue.Modifications = new DynamicJsonValue
                {
                    [Value.Name] = Value.ToJson()
                };

                return context.ReadObject(previousValue, Name);
            }

            var djv = new DynamicJsonValue
            {
                [Value.Name] = Value.ToJson()
            };

            return context.ReadObject(djv, Name);
        }

        private string GenerateTaskName(BlittableJsonReaderObject previousValue)
        {
            var baseTaskName = Value.GetDefaultTaskName();
            if (previousValue == null)
                return baseTaskName;

            long i = 1;
            var taskName = baseTaskName;
            var allTaskNames = previousValue.GetPropertyNames();
            while (allTaskNames.Contains(taskName, StringComparer.OrdinalIgnoreCase))
            {
                taskName += $" #{++i}";
            }

            return taskName;
        }

        public override void VerifyCanExecuteCommand(ServerStore store, TransactionOperationContext context, bool isClusterAdmin)
        {
            AssertClusterAdmin(isClusterAdmin);
        }

        public static string GetTaskNameForDatabase(string backupConfigurationName)
        {
            return $"{ServerWideBackupConfiguration.NamePrefix}, {backupConfigurationName}";
        }

        public static void UpdateTemplateForDatabase(PeriodicBackupConfiguration configuration, string databaseName)
        {
            configuration.Name = GetTaskNameForDatabase(configuration.Name);

            UpdateSettingsForLocal(configuration.LocalSettings, databaseName);

            UpdateSettingsForS3(configuration.S3Settings, databaseName);

            UpdateSettingsForAzure(configuration.AzureSettings, databaseName);

            UpdateSettingsForGoogleCloud(configuration.GoogleCloudSettings, databaseName);

            UpdateSettingsForFtp(configuration.FtpSettings, databaseName);
        }

        public static FtpSettings UpdateSettingsForFtp(FtpSettings ftpSettings, string databaseName)
        {
            if (ftpSettings?.Url != null)
            {
                ftpSettings.Url = GetUpdatedPath(ftpSettings.Url, databaseName);
            }

            return ftpSettings;
        }

        public static GoogleCloudSettings UpdateSettingsForGoogleCloud(GoogleCloudSettings googleCloudSettings, string databaseName)
        {
            if (googleCloudSettings != null)
            {
                googleCloudSettings.RemoteFolderName = GetUpdatedPath(googleCloudSettings.RemoteFolderName, databaseName);
            }

            return googleCloudSettings;
        }

        public static AzureSettings UpdateSettingsForAzure(AzureSettings azureSettings, string databaseName)
        {
            if (azureSettings != null)
            {
                azureSettings.RemoteFolderName = GetUpdatedPath(azureSettings.RemoteFolderName, databaseName);
            }

            return azureSettings;
        }

        public static S3Settings UpdateSettingsForS3(S3Settings s3Settings, string databaseName)
        {
            if (s3Settings != null)
            {
                s3Settings.RemoteFolderName = GetUpdatedPath(s3Settings.RemoteFolderName, databaseName);
            }

            return s3Settings;
        }

        public static LocalSettings UpdateSettingsForLocal(LocalSettings localSettings, string databaseName)
        {
            if (localSettings?.FolderPath != null)
            {
                localSettings.FolderPath = GetUpdatedPath(localSettings.FolderPath, databaseName, Path.DirectorySeparatorChar);
            }

            return localSettings;
        }

        private static string GetUpdatedPath(string str, string databaseName, char separator = '/')
        {
            if (str == null)
                return databaseName;

            if (str.EndsWith(separator) == false)
                str += separator;

            return str + databaseName;
        }
    }
}
