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
        public PutServerWideBackupConfigurationCommand()
        {
            Name = ClusterStateMachine.ServerWideBackupConfigurationsKey;
        }

        public PutServerWideBackupConfigurationCommand(ServerWideBackupConfiguration configuration) : this()
        {
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
