using System;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.Backups.Sharding;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Sparrow.Utils;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Sharding.Backup
{
    public class ShardedRestoreBackupTests : RavenTestBase
    {
        private readonly string _restoreFromS3TestsPrefix = $"sharding/tests/backup-restore/{nameof(ShardedRestoreBackupTests)}-{Guid.NewGuid()}";

        public ShardedRestoreBackupTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.BackupExportImport | RavenTestCategory.Sharding)]
        public async Task CanBackupAndRestoreSharded_Local()
        {
            using (var store1 = Sharding.GetDocumentStore())
            using (var store2 = GetDocumentStore())
            {
                await Sharding.Backup.InsertData(store1);
                var waitHandles = await Sharding.Backup.WaitForBackupToComplete(store1);

                var backupPath = NewDataPath(suffix: "BackupFolder");
                
                var config = Backup.CreateBackupConfiguration(backupPath);
                await Sharding.Backup.UpdateConfigurationAndRunBackupAsync(Server, store1, config);

                Assert.True(WaitHandle.WaitAll(waitHandles, TimeSpan.FromMinutes(1)));

                var dirs = Directory.GetDirectories(backupPath);
                Assert.Equal(3, dirs.Length);
                
                var settings = new ShardedRestoreSettings
                {
                    Shards = new SingleShardRestoreSetting[dirs.Length]
                };

                for (var i = 0; i < dirs.Length; i++)
                {
                    var dir = dirs[i];
                    settings.Shards[i] = new SingleShardRestoreSetting
                    {
                        ShardNumber = i, 
                        BackupPath = dir, 
                        NodeTag = "A"
                    };
                }

                // restore the database with a different name
                var databaseName = $"restored_database-{Guid.NewGuid()}";
                using (Sharding.Backup.ReadOnly(backupPath))
                using (Backup.RestoreDatabase(store2, new RestoreBackupConfiguration
                {
                    DatabaseName = databaseName,
                    ShardRestoreSettings = settings

                }, timeout: TimeSpan.FromSeconds(60)))
                {
                    var dbRec = await store2.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(databaseName));
                    Assert.Equal(DatabaseStateStatus.Normal, dbRec.DatabaseState);
                    Assert.Equal(3, dbRec.Sharding.Shards.Length);

                    await Sharding.Backup.CheckData(store2, RavenDatabaseMode.Sharded, expectedRevisionsCount : 16, database: databaseName);
                }
            }
        }

        [AmazonS3Fact(Skip = "not yet implemented")]
        public async Task CanBackupAndRestoreSharded_S3()
        {
            var s3Settings = GetS3Settings();
            try
            {
                var backupPath = NewDataPath(suffix: "BackupFolder");

                using (var store1 = Sharding.GetDocumentStore())
                using (var store2 = GetDocumentStore())
                {
                    await Sharding.Backup.InsertData(store1);

                    var waitHandles = await Sharding.Backup.WaitForBackupToComplete(store1);

                    var config = Backup.CreateBackupConfiguration(s3Settings: s3Settings);
                    await Sharding.Backup.UpdateConfigurationAndRunBackupAsync(Server, store1, config);

                    Assert.True(WaitHandle.WaitAll(waitHandles, TimeSpan.FromMinutes(1)));

                    var dirs = Directory.GetDirectories(backupPath);
                    Assert.Equal(3, dirs.Length);

                    var settings = new ShardedRestoreSettings
                    {
                        Shards = new SingleShardRestoreSetting[dirs.Length]
                    };

                    for (var i = 0; i < dirs.Length; i++)
                    {
                        var dir = dirs[i];
                        settings.Shards[i] = new SingleShardRestoreSetting
                        {
                            ShardNumber = i,
                            BackupPath = dir,
                            NodeTag = "A"
                        };
                    }

                    // restore the database with a different name
                    var databaseName = $"restored_database-{Guid.NewGuid()}";
                    using (Sharding.Backup.ReadOnly(backupPath))
                    using (Backup.RestoreDatabaseFromCloud(store2, new RestoreFromS3Configuration
                    {
                        DatabaseName = databaseName,
                        ShardRestoreSettings = settings,
                        Settings = s3Settings
                    }, timeout: TimeSpan.FromSeconds(60)))
                    {
                        var dbRec = await store2.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(databaseName));

                        Assert.Equal(DatabaseStateStatus.Normal, dbRec.DatabaseState);
                        Assert.Equal(3, dbRec.Sharding.Shards.Length);

                        await Sharding.Backup.CheckData(store2, RavenDatabaseMode.Sharded, database: databaseName);
                    }
                }
            }
            finally
            {
                // todo delete backup files from s3 
            }
        }

        private S3Settings GetS3Settings([CallerMemberName] string caller = null)
        {
            var s3Settings = AmazonS3FactAttribute.S3Settings;
            if (s3Settings == null)
                return null;

            var remoteFolderName = _restoreFromS3TestsPrefix;
            if (string.IsNullOrEmpty(caller) == false)
                remoteFolderName = $"{remoteFolderName}/{caller}";

            if (string.IsNullOrEmpty(s3Settings.RemoteFolderName) == false)
                remoteFolderName = $"{s3Settings.RemoteFolderName}/{remoteFolderName}";


            return new S3Settings
            {
                BucketName = s3Settings.BucketName,
                RemoteFolderName = remoteFolderName,
                AwsAccessKey = s3Settings.AwsAccessKey,
                AwsSecretKey = s3Settings.AwsSecretKey,
                AwsRegionName = s3Settings.AwsRegionName
            };
        }
    }
}
