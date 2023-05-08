using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using Sparrow.Json.Parsing;
using static Raven.Client.Documents.Operations.Backups.BackupConfiguration;

namespace Raven.Client.Documents.Operations.Backups
{
    public class BackupConfiguration : IDynamicJson
    {
        public BackupType BackupType { get; set; }
        public SnapshotSettings SnapshotSettings { get; set; }
        public BackupEncryptionSettings BackupEncryptionSettings { get; set; }
        public LocalSettings LocalSettings { get; set; }
        public S3Settings S3Settings { get; set; }
        public GlacierSettings GlacierSettings { get; set; }
        public AzureSettings AzureSettings { get; set; }
        public FtpSettings FtpSettings { get; set; }
        public GoogleCloudSettings GoogleCloudSettings { get; set; }

        internal bool HasBackup()
        {
            return CanBackupUsing(LocalSettings) ||
                   CanBackupUsing(S3Settings) ||
                   CanBackupUsing(GlacierSettings) ||
                   CanBackupUsing(AzureSettings) ||
                   CanBackupUsing(GoogleCloudSettings) ||
                   CanBackupUsing(FtpSettings);
        }

        internal bool HasCloudBackup()
        {
            return CanBackupUsing(S3Settings) ||
                   CanBackupUsing(GlacierSettings) ||
                   CanBackupUsing(AzureSettings) ||
                   CanBackupUsing(GoogleCloudSettings) ||
                   CanBackupUsing(FtpSettings);
        }

        internal static bool CanBackupUsing(BackupSettings settings)
        {
            return settings != null &&
                   settings.Disabled == false &&
                   settings.HasSettings();
        }

        internal List<string> GetFullBackupDestinations()
        {
            // used for studio and generating the default task name
            return GetBackupDestinations(AddBackupDestination).Select(str =>
            {
                var fieldInfo = typeof(PeriodicBackupConfiguration.BackupDestination).GetField(str);
                var attributes = (DescriptionAttribute[])fieldInfo.GetCustomAttributes(typeof(DescriptionAttribute), false);
                return attributes.Length > 0 ? attributes[0].Description : str;
            }).ToList();
        }
        
        internal List<string> GetDestinations()
        {
            return GetBackupDestinations(AddBackupDestination);
        }

        internal delegate void AddBackupDestinationDel(BackupSettings backupSettings, BackupDestination backupDestination,List<string> destList);
        
        private static void AddBackupDestination(BackupSettings backupSettings, BackupDestination backupDestination, List<string> destList)
        {
            if (backupSettings == null || backupSettings.Disabled)
                return;
        
            destList.Add(backupDestination.ToString());
        }

        internal static void AddBackupSettings(BackupSettings backupSettings, BackupDestination backupDestination, List<string> destList)
        {
            if (backupSettings == null)
                return;
            if (backupSettings.GetBackupConfigurationScript == null)
                return;
            destList.Add(backupDestination.ToString());
        }

        internal List<string> GetBackupDestinations(AddBackupDestinationDel addBackupDestination)
        {
            var backupDestinations = new List<string>();

            addBackupDestination(LocalSettings, BackupDestination.Local, backupDestinations);
            addBackupDestination(S3Settings, BackupDestination.AmazonS3, backupDestinations);
            addBackupDestination(GlacierSettings, BackupDestination.AmazonGlacier, backupDestinations);
            addBackupDestination(AzureSettings, BackupDestination.Azure, backupDestinations);
            addBackupDestination(GoogleCloudSettings, BackupDestination.GoogleCloud, backupDestinations);
            addBackupDestination(FtpSettings, BackupDestination.FTP, backupDestinations);

            return backupDestinations;
        }

        internal enum BackupDestination
        {
            None,
            Local,
            [Description("Amazon S3")]
            AmazonS3,
            [Description("Amazon Glacier")]
            AmazonGlacier,
            Azure,
            [Description("Google Cloud")]
            GoogleCloud,
            FTP
        }

        public virtual DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(BackupType)] = BackupType,
                [nameof(SnapshotSettings)] = SnapshotSettings?.ToJson(),
                [nameof(BackupEncryptionSettings)] = BackupEncryptionSettings?.ToJson(),
                [nameof(LocalSettings)] = LocalSettings?.ToJson(),
                [nameof(S3Settings)] = S3Settings?.ToJson(),
                [nameof(GlacierSettings)] = GlacierSettings?.ToJson(),
                [nameof(AzureSettings)] = AzureSettings?.ToJson(),
                [nameof(GoogleCloudSettings)] = GoogleCloudSettings?.ToJson(),
                [nameof(FtpSettings)] = FtpSettings?.ToJson()
            };
        }
        public virtual DynamicJsonValue ToAuditJson()
        {
            return new DynamicJsonValue
            {
                [nameof(BackupType)] = BackupType,
                [nameof(SnapshotSettings)] = SnapshotSettings?.ToAuditJson(),
                [nameof(BackupEncryptionSettings)] = BackupEncryptionSettings?.ToAuditJson(),
                [nameof(LocalSettings)] = LocalSettings?.ToAuditJson(),
                [nameof(S3Settings)] = S3Settings?.ToAuditJson(),
                [nameof(GlacierSettings)] = GlacierSettings?.ToAuditJson(),
                [nameof(AzureSettings)] = AzureSettings?.ToAuditJson(),
                [nameof(GoogleCloudSettings)] = GoogleCloudSettings?.ToAuditJson(),
                [nameof(FtpSettings)] = FtpSettings?.ToAuditJson()

            };
        }

        public virtual bool ValidateDestinations(out string message)
        {
            if (HasBackup() == false)
            {
                message = "The backup configuration target no destinations";
                return false;
            }
            message = null;
            return true;
        }
    }
}
