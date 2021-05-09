using System;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Exceptions;
using Raven.Client.ServerWide.Operations;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.PeriodicBackup.Restore
{
    public class RestoreFromAwsS3 : RestoreFromS3
    {
        public RestoreFromAwsS3(ITestOutputHelper output) : base(output)
        {
        }

        [Fact, Trait("Category", "Smuggler")]
        public void restore_s3_settings_tests()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");
            using (var store = GetDocumentStore(new Options
            {
                ModifyDatabaseName = s => $"{s}_2"
            }))
            {
                var databaseName = $"restored_database-{Guid.NewGuid()}";
                var restoreConfiguration = new RestoreFromS3Configuration
                {
                    DatabaseName = databaseName
                };

                var restoreBackupTask = new RestoreBackupOperation(restoreConfiguration);

                var e = Assert.Throws<RavenException>(() => store.Maintenance.Server.Send(restoreBackupTask));
                Assert.Contains("AWS access key cannot be null or empty", e.InnerException.Message);

                restoreConfiguration.Settings.AwsAccessKey = "test";
                restoreBackupTask = new RestoreBackupOperation(restoreConfiguration);
                e = Assert.Throws<RavenException>(() => store.Maintenance.Server.Send(restoreBackupTask));
                Assert.Contains("AWS secret key cannot be null or empty", e.InnerException.Message);

                restoreConfiguration.Settings.AwsSecretKey = "test";
                restoreBackupTask = new RestoreBackupOperation(restoreConfiguration);
                e = Assert.Throws<RavenException>(() => store.Maintenance.Server.Send(restoreBackupTask));
                Assert.Contains("AWS Bucket name cannot be null or empty", e.InnerException.Message);

                restoreConfiguration.Settings.BucketName = "test";
                restoreBackupTask = new RestoreBackupOperation(restoreConfiguration);
                e = Assert.Throws<RavenException>(() => store.Maintenance.Server.Send(restoreBackupTask));
                Assert.Contains("AWS region name cannot be null or empty", e.InnerException.Message);
            }
        }
        
        [AmazonS3Fact, Trait("Category", "Smuggler")]
        public async Task can_backup_and_restore() => await can_backup_and_restore_internal();
        [AmazonS3Fact, Trait("Category", "Smuggler")]
        public async Task can_backup_and_restore_snapshot() => await can_backup_and_restore_snapshot_internal();
        [AmazonS3Fact, Trait("Category", "Smuggler")]
        public async Task incremental_and_full_backup_encrypted_db_and_restore_to_encrypted_DB_with_database_key() => 
            await incremental_and_full_backup_encrypted_db_and_restore_to_encrypted_DB_with_database_key_internal();
        [AmazonS3Fact, Trait("Category", "Smuggler")]
        public async Task incremental_and_full_check_last_file_for_backup() => await incremental_and_full_check_last_file_for_backup_internal();
        [AmazonS3Fact, Trait("Category", "Smuggler")]
        public async Task incremental_and_full_backup_encrypted_db_and_restore_to_encrypted_DB_with_provided_key() => 
            await incremental_and_full_backup_encrypted_db_and_restore_to_encrypted_DB_with_provided_key_internal();
        [AmazonS3Fact, Trait("Category", "Smuggler")]
        public async Task snapshot_encrypted_db_and_restore_to_encrypted_DB() => await snapshot_encrypted_db_and_restore_to_encrypted_DB_internal();
    }
}
