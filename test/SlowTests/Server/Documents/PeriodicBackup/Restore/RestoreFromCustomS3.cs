using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Backups;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.PeriodicBackup.Restore
{
    public class RestoreFromCustomS3 : RestoreFromS3
    {
        public RestoreFromCustomS3(ITestOutputHelper output) : base(output, isCustom: true)
        {
        }

        [CustomS3RetryTheory, Trait("Category", "Smuggler")]
        [InlineData(BackupUploadMode.Default)]
        [InlineData(BackupUploadMode.DirectUpload)]
        public async Task can_backup_and_restore(BackupUploadMode backupUploadMode) => await can_backup_and_restore_internal(backupUploadMode: backupUploadMode);

        [CustomS3RetryTheory, Trait("Category", "Smuggler")]
        [InlineData(BackupUploadMode.Default)]
        [InlineData(BackupUploadMode.DirectUpload)]
        public async Task can_backup_and_restore_snapshot(BackupUploadMode backupUploadMode) => await can_backup_and_restore_snapshot_internal(backupUploadMode: backupUploadMode);

        [CustomS3RetryTheory, Trait("Category", "Smuggler")]
        [InlineData(BackupUploadMode.Default)]
        [InlineData(BackupUploadMode.DirectUpload)]
        public async Task incremental_and_full_backup_encrypted_db_and_restore_to_encrypted_DB_with_database_key(BackupUploadMode backupUploadMode) => 
            await incremental_and_full_backup_encrypted_db_and_restore_to_encrypted_DB_with_database_key_internal(backupUploadMode: backupUploadMode);

        [CustomS3RetryFact, Trait("Category", "Smuggler")]
        public async Task incremental_and_full_check_last_file_for_backup() => await incremental_and_full_check_last_file_for_backup_internal();

        [CustomS3RetryFact, Trait("Category", "Smuggler")]
        public async Task incremental_and_full_backup_encrypted_db_and_restore_to_encrypted_DB_with_provided_key() => 
            await incremental_and_full_backup_encrypted_db_and_restore_to_encrypted_DB_with_provided_key_internal();

        [CustomS3RetryTheory, Trait("Category", "Smuggler")]
        [InlineData(BackupUploadMode.Default)]
        [InlineData(BackupUploadMode.DirectUpload)]
        public async Task snapshot_encrypted_db_and_restore_to_encrypted_DB(BackupUploadMode backupUploadMode) => await snapshot_encrypted_db_and_restore_to_encrypted_DB_internal(backupUploadMode: backupUploadMode);
    }
}
