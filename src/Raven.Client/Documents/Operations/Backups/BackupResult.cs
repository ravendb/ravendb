using Raven.Client.Documents.Smuggler;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.Backups
{
    public class BackupResult : SmugglerResult
    {
        public Counts SnapshotBackup { get; set; }

        public UploadToS3 S3Backup { get; set; }

        public UploadToAzure AzureBackup { get; set; }
        
        public UploadToGoogleCloud GoogleCloudBackup { get; set; }

        public UploadToGlacier GlacierBackup { get; set; }

        public UploadToFtp FtpBackup { get; set; }

        public string FolderName { get; set; }
        public string FileName { get; set; }

        public BackupResult()
        {
            _progress = new BackupProgress(this);
            SnapshotBackup = new Counts();
            S3Backup = new UploadToS3();
            AzureBackup = new UploadToAzure();
            GoogleCloudBackup = new UploadToGoogleCloud();
            GlacierBackup = new UploadToGlacier();
            FtpBackup = new UploadToFtp();
        }

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            json[nameof(SnapshotBackup)] = SnapshotBackup.ToJson();
            json[nameof(S3Backup)] = S3Backup.ToJson();
            json[nameof(AzureBackup)] = AzureBackup.ToJson();
            json[nameof(GoogleCloudBackup)] = GoogleCloudBackup.ToJson();
            json[nameof(GlacierBackup)] = GlacierBackup.ToJson();
            json[nameof(FtpBackup)] = FtpBackup.ToJson();
            json[nameof(FolderName)] = FolderName;
            json[nameof(FileName)] = FileName;
            return json;
        }
    }
}
