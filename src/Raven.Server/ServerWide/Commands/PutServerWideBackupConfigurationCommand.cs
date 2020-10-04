using System;
using System.IO;
using System.Linq;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.ServerWide.Operations.Configuration;
using Raven.Server.Rachis;
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
            Name = ClusterStateMachine.ServerWideConfigurationKey.Backup;
            Value = configuration;
        }

        public override object ValueToJson()
        {
            return Value.ToJson();
        }

        public override BlittableJsonReaderObject GetUpdatedValue(JsonOperationContext context, BlittableJsonReaderObject previousValue, long index)
        {
            if (string.IsNullOrWhiteSpace(Value.Name))
                Value.Name = GenerateTaskName(previousValue);

            if (Value.ExcludedDatabases != null &&
                Value.ExcludedDatabases.Any(string.IsNullOrWhiteSpace))
                throw new RachisApplyException($"{nameof(ServerWideBackupConfiguration.ExcludedDatabases)} cannot contain null or empty database names");

            var prevTaskId = Value.TaskId;
            Value.TaskId = index;

            string oldName = null;
            if (previousValue != null)
            {
                previousValue.Modifications ??= new DynamicJsonValue();

                if (previousValue.TryGet(Value.Name, out object _) == false)
                {
                    if (prevTaskId != 0)
                    {
                        foreach (var propertyName in previousValue.GetPropertyNames())
                        {
                            var property = (BlittableJsonReaderObject)previousValue[propertyName];
                            if (property.TryGet(nameof(ServerWideBackupConfiguration.TaskId), out long taskId) == false)
                                throw new RachisInvalidOperationException(
                                    $"Current {nameof(ServerWideBackupConfiguration)} has no {nameof(ServerWideBackupConfiguration.TaskId)} " +
                                    $"or the {nameof(ServerWideBackupConfiguration.TaskId)} is not of the correct type. " +
                                    $"Should not happen\n {previousValue}");

                            if (taskId != prevTaskId)
                                continue;

                            oldName = propertyName;
                            break;
                        }

                        if (oldName == null)
                            throw new RachisInvalidOperationException(
                                $"Can't find {nameof(ServerWideBackupConfiguration)} with {nameof(ServerWideBackupConfiguration.Name)} {Value.Name} or with {nameof(ServerWideBackupConfiguration.TaskId)} {prevTaskId}. " +
                                $"If you try to create new {nameof(ServerWideBackupConfiguration)} set the {nameof(ServerWideBackupConfiguration.TaskId)} to 0." +
                                $"If you try to update exist {nameof(ServerWideBackupConfiguration)} and change its name, request the configuration again from the server to get the current {nameof(ServerWideBackupConfiguration.TaskId)} and send the {nameof(PutServerWideBackupConfigurationCommand)} again");
                    }
                }

                var modifications = new DynamicJsonValue(previousValue);
                if (oldName != null && oldName.Equals(Value.Name) == false)
                    modifications.Remove(oldName);

                modifications[Value.Name] = Value.ToJson();
                var blittableJsonReaderObject = context.ReadObject(previousValue, Name);
                return blittableJsonReaderObject;
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

        public static string GetTaskName(string backupConfigurationName)
        {
            return $"{ServerWideBackupConfiguration.NamePrefix}, {backupConfigurationName}";
        }

        public static void UpdateTemplateForDatabase(PeriodicBackupConfiguration configuration, string databaseName, bool isDatabaseEncrypted)
        {
            configuration.Name = GetTaskName(configuration.Name);

            UpdateSettingsForLocal(configuration.LocalSettings, databaseName);

            UpdateSettingsForS3(configuration.S3Settings, databaseName);

            UpdateSettingsForGlacier(configuration.GlacierSettings, databaseName);

            UpdateSettingsForAzure(configuration.AzureSettings, databaseName);

            UpdateSettingsForFtp(configuration.FtpSettings, databaseName);

            UpdateSettingsForGoogleCloud(configuration.GoogleCloudSettings, databaseName);

            if (isDatabaseEncrypted)
            {
                // if the database is encrypted, the backup should be encrypted as well
                configuration.BackupEncryptionSettings = new BackupEncryptionSettings
                {
                    EncryptionMode = EncryptionMode.UseDatabaseKey
                };
            }
            else if (configuration.BackupType == BackupType.Snapshot)
            {
                // the database isn't encrypted and the backup is of type snapshot
                // in this case we cannot have an encrypted snapshot
                configuration.BackupEncryptionSettings = null;
            }
        }

        public static void UpdateSettingsForLocal(LocalSettings localSettings, string databaseName)
        {
            if (localSettings?.FolderPath != null)
            {
                localSettings.FolderPath = GetUpdatedPath(localSettings.FolderPath, databaseName, Path.DirectorySeparatorChar);
            }
        }

        public static void UpdateSettingsForS3(S3Settings s3Settings, string databaseName)
        {
            if (s3Settings != null)
            {
                s3Settings.RemoteFolderName = GetUpdatedPath(s3Settings.RemoteFolderName, databaseName);
            }
        }

        public static void UpdateSettingsForGlacier(GlacierSettings glacierSettings, string databaseName)
        {
            if (glacierSettings != null)
            {
                glacierSettings.RemoteFolderName = GetUpdatedPath(glacierSettings.RemoteFolderName, databaseName);
            }
        }

        public static void UpdateSettingsForAzure(AzureSettings azureSettings, string databaseName)
        {
            if (azureSettings != null)
            {
                azureSettings.RemoteFolderName = GetUpdatedPath(azureSettings.RemoteFolderName, databaseName);
            }
        }

        public static void UpdateSettingsForFtp(FtpSettings ftpSettings, string databaseName)
        {
            if (ftpSettings?.Url != null)
            {
                ftpSettings.Url = GetUpdatedPath(ftpSettings.Url, databaseName);
            }
        }

        public static void UpdateSettingsForGoogleCloud(GoogleCloudSettings googleCloudSettings, string databaseName)
        {
            if (googleCloudSettings != null)
            {
                googleCloudSettings.RemoteFolderName = GetUpdatedPath(googleCloudSettings.RemoteFolderName, databaseName);
            }
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
