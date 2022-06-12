using System;
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

        public override bool CanMerge => true;

        public static void MergeCloudUploadStatus(CloudUploadStatus finalResult, CloudUploadStatus result)
        {
            //TODO stav: impl
            finalResult.Skipped = result.Skipped;
            finalResult.LastFullBackup = result.LastFullBackup;
            finalResult.LastIncrementalBackup = result.LastIncrementalBackup;
        }

        public override void MergeWith(IOperationResult result)
        {
            if (result is not BackupResult br)
                return;
            
            base.MergeWith(result);
            MergeBackupResult(this, br);
        }

        public static void MergeBackupResult(BackupResult finalResult, BackupResult result)
        {
            //TODO stav: missing fields non mergeable. assign IOperationProgress to all field types?
            finalResult.SnapshotBackup.ErroredCount += result.SnapshotBackup.ErroredCount;
            finalResult.SnapshotBackup.ReadCount += result.SnapshotBackup.ReadCount;
            //SnapshotBackup: (bool) Processed, (bool) Skipped, StartTime

            MergeCloudUploadStatus(finalResult.S3Backup, result.S3Backup);
            MergeCloudUploadStatus(finalResult.AzureBackup, result.AzureBackup);
            MergeCloudUploadStatus(finalResult.GoogleCloudBackup, result.GoogleCloudBackup);
            MergeCloudUploadStatus(finalResult.GlacierBackup, result.GlacierBackup);
            MergeCloudUploadStatus(finalResult.FtpBackup, result.FtpBackup);

            //LocalBackup.
        }
    }

    public class ShardedBackupResult : BackupResult, IShardedOperationResult
    {
        public IShardNodeIdentifier[] Results { get; set; }

        public void CombineWith(IOperationResult result, int shardNumber, string nodeTag)
        {
            Results[shardNumber] = new ShardedNodeBackupResult
            {
                ShardResult = result,
                ShardNumber = shardNumber,
                NodeTag = nodeTag
            };
        }

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            json[nameof(Results)] = new DynamicJsonArray(Results.Select(x => x.ToJson()));
            return json;
        }
    }

    public class ShardedNodeBackupResult : IShardNodeIdentifier
    {
        public int ShardNumber { get; set; }
        public string NodeTag { get; set; }
        public IOperationResult ShardResult { get; set; }

        public string Message { get; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue(GetType()) 
            {
                [nameof(ShardNumber)] = ShardNumber,
                [nameof(NodeTag)] = NodeTag,
                [nameof(ShardResult)] = ShardResult.ToJson()
            };
        }

        public bool ShouldPersist => false;
        public bool CanMerge => false;
        public void MergeWith(IOperationResult result)
        {
            throw new NotImplementedException();
        }
    }
}
