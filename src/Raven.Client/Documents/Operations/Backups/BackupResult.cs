using System;
using System.Collections.Generic;
using System.Linq;
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

        public LocalBackup LocalBackup { get; set; }

        public BackupResult()
        {
            _progress = new BackupProgress(this);
            SnapshotBackup = new Counts();
            S3Backup = new UploadToS3();
            AzureBackup = new UploadToAzure();
            GoogleCloudBackup = new UploadToGoogleCloud();
            GlacierBackup = new UploadToGlacier();
            FtpBackup = new UploadToFtp();
            LocalBackup = new LocalBackup();
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
            json[nameof(LocalBackup)] = LocalBackup.ToJson();
            return json;
        }
    }

    public class ShardedBackupResult : IShardedOperationResult<ShardNodeBackupResult>
    {
        public List<ShardNodeBackupResult> Results { get; set; }

        public ShardedBackupResult()
        {
            Message = null;
        }

        public void CombineWith(IOperationResult result, int shardNumber, string nodeTag)
        {
            Results ??= new List<ShardNodeBackupResult>();

            if (result is not BackupResult br)
                return;

            Results.Add(new ShardNodeBackupResult
            {
                Result = br,
                ShardNumber = shardNumber,
                NodeTag = nodeTag
            });
        }

        public string Message { get; private set; }

        public DynamicJsonValue ToJson()
        {
            var json = new DynamicJsonValue(GetType());
            json[nameof(Results)] = new DynamicJsonArray(Results.Select(x => x.ToJson()));
            return json;
        }

        public bool ShouldPersist => false;
        public bool CanMerge => false;
        public void MergeWith(IOperationResult result)
        {
            throw new NotSupportedException();
        }
    }

    public class ShardNodeBackupResult : ShardNodeOperationResult<BackupResult>
    {
        public override bool ShouldPersist => true;
    }
}
