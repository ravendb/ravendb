using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Backups;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.PeriodicBackup.Restore
{
    public class RestoreFromCustomS3 : RestoreFromS3
    {
        public RestoreFromCustomS3(ITestOutputHelper output) : base(output)
        {
        }

        [CustomS3Fact, Trait("Category", "Smuggler")]
        public async Task can_backup_and_restore() => await can_backup_and_restore_internal();
        [CustomS3Fact, Trait("Category", "Smuggler")]
        public async Task can_backup_and_restore_snapshot() => await can_backup_and_restore_snapshot_internal();
        [CustomS3Fact, Trait("Category", "Smuggler")]
        public async Task incremental_and_full_backup_encrypted_db_and_restore_to_encrypted_DB_with_database_key() => 
            await incremental_and_full_backup_encrypted_db_and_restore_to_encrypted_DB_with_database_key_internal();
        [CustomS3Fact, Trait("Category", "Smuggler")]
        public async Task incremental_and_full_check_last_file_for_backup() => await incremental_and_full_check_last_file_for_backup_internal();
        [CustomS3Fact, Trait("Category", "Smuggler")]
        public async Task incremental_and_full_backup_encrypted_db_and_restore_to_encrypted_DB_with_provided_key() => 
            await incremental_and_full_backup_encrypted_db_and_restore_to_encrypted_DB_with_provided_key_internal();
        [CustomS3Fact, Trait("Category", "Smuggler")]
        public async Task snapshot_encrypted_db_and_restore_to_encrypted_DB() => await snapshot_encrypted_db_and_restore_to_encrypted_DB_internal();

        protected override S3Settings GetS3Settings(string subPath = null)
        {
            var s3Settings = CustomS3FactAttribute.S3Settings;

            if (s3Settings == null)
                return null;

            var remoteFolderName = $"{s3Settings.RemoteFolderName}/{_cloudPathPrefix}";

            if (string.IsNullOrEmpty(subPath) == false)
                remoteFolderName = $"{remoteFolderName}/{subPath}";

            return new S3Settings
            {
                BucketName = s3Settings.BucketName,
                RemoteFolderName = remoteFolderName,
                AwsAccessKey = s3Settings.AwsAccessKey,
                AwsSecretKey = s3Settings.AwsSecretKey,
                CustomServerUrl = s3Settings.CustomServerUrl,
                AwsRegionName = s3Settings.AwsRegionName,
            };
        }
    }
}
