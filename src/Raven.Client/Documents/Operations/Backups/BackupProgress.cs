using Raven.Client.Documents.Smuggler;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.Backups
{
    public class BackupProgress : SmugglerResult.SmugglerProgress
    {
        public Counts SnapshotBackup => ((BackupResult)_result).SnapshotBackup;

        public UploadToS3 S3Backup => ((BackupResult)_result).S3Backup;

        public UploadToAzure AzureBackup => ((BackupResult)_result).AzureBackup;

        public UploadToGlacier GlacierBackup => ((BackupResult)_result).GlacierBackup;

        public UploadToFtp FtpBackup => ((BackupResult)_result).FtpBackup;

        public BackupProgress(BackupResult result) : base(result)
        {

        }

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            json[nameof(SnapshotBackup)] = SnapshotBackup.ToJson();
            json[nameof(S3Backup)] = S3Backup.ToJson();
            json[nameof(AzureBackup)] = AzureBackup.ToJson();
            json[nameof(GlacierBackup)] = GlacierBackup.ToJson();
            json[nameof(FtpBackup)] = FtpBackup.ToJson();
            return json;
        }
    }
}
