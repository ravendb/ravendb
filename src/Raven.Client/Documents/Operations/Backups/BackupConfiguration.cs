using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using Sparrow.Json.Parsing;

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
            return GetBackupDestinations().Select(backupDestination =>
            {
                var str = backupDestination.ToString();
                var fieldInfo = typeof(PeriodicBackupConfiguration.BackupDestination).GetField(str);
                var attributes = (DescriptionAttribute[])fieldInfo.GetCustomAttributes(typeof(DescriptionAttribute), false);
                return attributes.Length > 0 ? attributes[0].Description : str;
            }).ToList();
        }

        internal List<string> GetDestinations()
        {
            return GetBackupDestinations().Select(x => x.ToString()).ToList();
        }

        private List<BackupDestination> GetBackupDestinations()
        {
            var backupDestinations = new List<BackupDestination>();

            AddBackupDestination(LocalSettings, BackupDestination.Local);
            AddBackupDestination(S3Settings, BackupDestination.AmazonS3);
            AddBackupDestination(GlacierSettings, BackupDestination.AmazonGlacier);
            AddBackupDestination(AzureSettings, BackupDestination.Azure);
            AddBackupDestination(GoogleCloudSettings, BackupDestination.GoogleCloud);
            AddBackupDestination(FtpSettings, BackupDestination.FTP);

            void AddBackupDestination(BackupSettings backupSettings, BackupDestination backupDestination)
            {
                if (backupSettings == null || backupSettings.Disabled)
                    return;

                backupDestinations.Add(backupDestination);
            }

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
