using Raven.Client.Documents.Smuggler;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.Backups
{
    public class BackupProgress : SmugglerResult.SmugglerProgress
    {
        public Counts SnapshotBackup => (_result as BackupResult)?.SnapshotBackup;

        public UploadToS3 S3Backup => (_result as BackupResult)?.S3Backup;

        public UploadToAzure AzureBackup => (_result as BackupResult)?.AzureBackup;

        public UploadToGoogleCloud GoogleCloudBackup => (_result as BackupResult)?.GoogleCloudBackup;

        public UploadToGlacier GlacierBackup => (_result as BackupResult)?.GlacierBackup;

        public UploadToFtp FtpBackup => (_result as BackupResult)?.FtpBackup;

        public BackupProgress(BackupResult result) : base(result)
        {
        }

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            json[nameof(SnapshotBackup)] = SnapshotBackup?.ToJson();
            json[nameof(S3Backup)] = S3Backup?.ToJson();
            json[nameof(AzureBackup)] = AzureBackup?.ToJson();
            json[nameof(GoogleCloudBackup)] = GoogleCloudBackup?.ToJson();
            json[nameof(GlacierBackup)] = GlacierBackup?.ToJson();
            json[nameof(FtpBackup)] = FtpBackup?.ToJson();
            return json;
        }
    }

    public sealed class ShardedBackupProgress : BackupProgress, IShardedOperationProgress
    {
        public int ShardNumber { get; set; }
        public string NodeTag { get; set; }

        public ShardedBackupProgress() : base(null)
        {
        }
        
        public ShardedBackupProgress(BackupResult result) : base(result)
        {
        }
        
        public void Fill(IOperationProgress progress, int shardNumber, string nodeTag)
        {
            ShardNumber = shardNumber;
            NodeTag = nodeTag;

            if (progress is not BackupProgress bp)
                return;

            _result = bp._result;
            DatabaseRecord = bp.DatabaseRecord;
            Documents = bp.Documents;
            RevisionDocuments = bp.RevisionDocuments;
            Tombstones = bp.Tombstones;
            Conflicts = bp.Conflicts;
            Identities = bp.Identities;
            Indexes = bp.Indexes;
            CompareExchange = bp.CompareExchange;
            Subscriptions = bp.Subscriptions;
            ReplicationHubCertificates = bp.ReplicationHubCertificates;
            Counters = bp.Counters;
            TimeSeries = bp.TimeSeries;
            CompareExchangeTombstones = bp.CompareExchangeTombstones;
            TimeSeriesDeletedRanges = bp.TimeSeriesDeletedRanges;
        }

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            json[nameof(ShardNumber)] = ShardNumber;
            json[nameof(NodeTag)] = NodeTag;
            return json;
        }
    }
}
