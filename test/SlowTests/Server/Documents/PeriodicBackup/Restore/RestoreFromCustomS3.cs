using System.Runtime.CompilerServices;
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
    }
}
