using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using NCrontab.Advanced;
using Raven.Client.Documents.Operations.Backups;
using Raven.Server.Config.Settings;
using Raven.Server.ServerWide;
using Raven.Server.Web.Studio;
using Voron.Util.Settings;

namespace Raven.Server.Documents.PeriodicBackup
{
    public static class BackupConfigurationHelper
    {
        internal static bool SkipMinimumBackupAgeToKeepValidation = false;

        public static async Task GetFullBackupDataDirectory(PathSetting path, string databaseName, int requestTimeoutInMs, bool getNodesInfo, ServerStore serverStore, Stream responseStream)
        {
            var isBackup = string.IsNullOrEmpty(databaseName) == false;
            var pathResult = GetActualFullPath(serverStore, path.FullPath);
            var info = new DataDirectoryInfo(serverStore, pathResult.FolderPath, databaseName, isBackup, getNodesInfo, requestTimeoutInMs, responseStream);
            await info.UpdateDirectoryResult(databaseName: databaseName, error: pathResult.Error);
        }
        
        public static void UpdateLocalPathIfNeeded(PeriodicBackupConfiguration configuration, ServerStore serverStore)
        {
            if (configuration.LocalSettings == null || configuration.LocalSettings.Disabled)
                return;

            var folderPath = configuration.LocalSettings.FolderPath;
            if (string.IsNullOrWhiteSpace(configuration.LocalSettings.FolderPath))
                return;

            var pathResult = GetActualFullPath(serverStore, folderPath);
            if (pathResult.Error != null)
                throw new ArgumentException(pathResult.Error);

            configuration.LocalSettings.FolderPath = pathResult.FolderPath;
        }

        public static ActualPathResult GetActualFullPath(ServerStore serverStore, string folderPath)
        {
            var pathResult = new ActualPathResult();
            if (serverStore.Configuration.Backup.LocalRootPath == null)
            {
                pathResult.FolderPath = folderPath;

                if (string.IsNullOrWhiteSpace(folderPath))
                {
                    pathResult.Error = "Backup directory cannot be null or empty";
                }

                return pathResult;
            }

            // in this case we receive a path relative to the root path
            try
            {
                pathResult.FolderPath = serverStore.Configuration.Backup.LocalRootPath.Combine(folderPath).FullPath;
            }
            catch
            {
                pathResult.Error = $"Unable to combine the local root path '{serverStore.Configuration.Backup.LocalRootPath?.FullPath}' " +
                                   $"with the user supplied relative path '{folderPath}'";
                return pathResult;
            }

            if (PathUtil.IsSubDirectory(pathResult.FolderPath, serverStore.Configuration.Backup.LocalRootPath.FullPath) == false)
            {
                pathResult.Error = $"The administrator has restricted local backups to be saved under the following root path '{serverStore.Configuration.Backup.LocalRootPath?.FullPath}' " +
                                   $"but the actual chosen path is '{pathResult.FolderPath}' which is not a sub-directory of the root path.";
                return pathResult;
            }

            pathResult.HasLocalRootPath = true;
            return pathResult;
        }

        public static void AssertBackupConfiguration(PeriodicBackupConfiguration configuration)
        {
            if (VerifyBackupFrequency(configuration.FullBackupFrequency) == null &&
                VerifyBackupFrequency(configuration.IncrementalBackupFrequency) == null)
            {
                throw new ArgumentException("Couldn't parse the cron expressions for both full and incremental backups. " +
                                            $"full backup cron expression: {configuration.FullBackupFrequency}, " +
                                            $"incremental backup cron expression: {configuration.IncrementalBackupFrequency}");
            }

            AssertBackupConfigurationInternal(configuration);
            AssertDirectUpload(configuration);

            var retentionPolicy = configuration.RetentionPolicy;
            if (retentionPolicy != null && retentionPolicy.Disabled == false)
            {
                if (retentionPolicy.MinimumBackupAgeToKeep != null)
                {
                    if (retentionPolicy.MinimumBackupAgeToKeep.Value.Ticks <= 0)
                        throw new ArgumentException($"{nameof(RetentionPolicy.MinimumBackupAgeToKeep)} must be positive");

                    if (SkipMinimumBackupAgeToKeepValidation == false && retentionPolicy.MinimumBackupAgeToKeep.Value < TimeSpan.FromDays(1))
                        throw new ArgumentException($"{nameof(RetentionPolicy.MinimumBackupAgeToKeep)} must be bigger than one day");
                }
            }

            CrontabSchedule VerifyBackupFrequency(string backupFrequency)
            {
                return string.IsNullOrWhiteSpace(backupFrequency) ? null : CrontabSchedule.Parse(backupFrequency);
            }
        }

        public static void AssertBackupConfigurationInternal(BackupConfiguration configuration)
        {
            var localSettings = configuration.LocalSettings;
            if (localSettings != null && localSettings.Disabled == false)
            {
                if (localSettings.HasSettings() == false)
                {
                    throw new ArgumentException(
                        $"{nameof(localSettings.FolderPath)} and {nameof(localSettings.GetBackupConfigurationScript)} cannot be both null or empty");
                }

                if (string.IsNullOrEmpty(localSettings.FolderPath) == false)
                {
                    if (DataDirectoryInfo.CanAccessPath(localSettings.FolderPath, out var error) == false)
                        throw new ArgumentException(error);
                }
            }
        }

        private static void AssertDirectUpload(PeriodicBackupConfiguration configuration)
        {
            if (configuration.BackupUploadMode != BackupUploadMode.DirectUpload)
                return;

            var backupToLocalFolder = BackupConfiguration.CanBackupUsing(configuration.LocalSettings);
            GetBackupDestinationForDirectUpload(backupToLocalFolder, configuration); // will throw if destination isn't set correctly
        }

        internal static BackupConfiguration.BackupDestination GetBackupDestinationForDirectUpload(bool backupToLocalFolder, BackupConfiguration configuration)
        {
            if (backupToLocalFolder)
            {
                throw new NotSupportedException("Trying to use direct upload when we configure a backup to a local folder.");
            }

            var hasAws = BackupConfiguration.CanBackupUsing(configuration.S3Settings);
            var hasGlacier = BackupConfiguration.CanBackupUsing(configuration.GlacierSettings);
            var hasAzure = BackupConfiguration.CanBackupUsing(configuration.AzureSettings);
            var hasGoogleCloud = BackupConfiguration.CanBackupUsing(configuration.GoogleCloudSettings);
            var hasFtp = BackupConfiguration.CanBackupUsing(configuration.FtpSettings);

            var destinations = new List<bool> { hasAws, hasGlacier, hasAzure, hasGoogleCloud, hasFtp };
            if (destinations.Count(x => x) != 1)
            {
                throw new NotSupportedException("Cannot use direct upload when we configure more than one destination.");
            }

            if (hasAws)
                return BackupConfiguration.BackupDestination.AmazonS3;

            throw new NotSupportedException("No supported backup destination for direct upload was set.");
        }

        public static void AssertDestinationAndRegionAreAllowed(BackupConfiguration configuration, ServerStore serverStore)
        {
            if (configuration.ValidateDestinations(out var errorMassage) == false)
                throw new InvalidOperationException(errorMassage);

            foreach (var backupDestination in configuration.GetDestinations())
            {
                serverStore.Configuration.Backup.AssertDestinationAllowed(backupDestination);
            }

            if (configuration.S3Settings != null && configuration.S3Settings.Disabled == false)
                serverStore.Configuration.Backup.AssertRegionAllowed(configuration.S3Settings.AwsRegionName);

            if (configuration.GlacierSettings != null && configuration.GlacierSettings.Disabled == false)
                serverStore.Configuration.Backup.AssertRegionAllowed(configuration.GlacierSettings.AwsRegionName);
        }

        public class ActualPathResult
        {
            public bool HasLocalRootPath { get; set; }

            public string FolderPath { get; set; }

            public string Error { get; set; }
        }
    }
}
