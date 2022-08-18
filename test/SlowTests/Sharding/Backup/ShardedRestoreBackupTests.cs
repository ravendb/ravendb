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
            var file = GetTempFileName();
            try
            {

                using (var store1 = Sharding.GetDocumentStore())
                using (var store2 = GetDocumentStore())
                {
                    await Sharding.Backup.InsertData(store1);
                    var waitHandles = await Sharding.Backup.WaitForBackupToComplete(store1);
                    //await Sharding.Backup.CheckData(store1, RavenDatabaseMode.Sharded);

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

                    }, timeout: TimeSpan.FromSeconds(6000))) //todo fix timeouts
                    {
                        var dbRec = await store2.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(databaseName));
                        Assert.Equal(DatabaseStateStatus.Normal, dbRec.DatabaseState);
                        Assert.Equal(3, dbRec.Sharding.Shards.Length);

                        //WaitForUserToContinueTheTest(store2, database: databaseName);

                        await Sharding.Backup.CheckData(store2, RavenDatabaseMode.Sharded, database: databaseName);
                        //await CheckData(Server.ServerStore, store3, names, databaseName);
                    }
                }
            }
            finally
            {
                File.Delete(file);
            }
        }

        [AmazonS3Fact(Skip = "not implemented")]
        [RavenFact(RavenTestCategory.BackupExportImport | RavenTestCategory.Sharding)]
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
                    //await InsertData(store1, names);

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
                    using (Backup.RestoreDatabase(store2, new RestoreBackupConfiguration
                    {
                        DatabaseName = databaseName,
                        ShardRestoreSettings = settings

                    }, timeout: TimeSpan.FromSeconds(6000))) //todo fix timeouts
                    {
                        var dbRec = await store2.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(databaseName));

                        Assert.Equal(DatabaseStateStatus.Normal, dbRec.DatabaseState);
                        Assert.Equal(3, dbRec.Sharding.Shards.Length);

                        WaitForUserToContinueTheTest(store2, database: databaseName);

                        await Sharding.Backup.CheckData(store2, RavenDatabaseMode.Sharded);
                        //await CheckData(Server.ServerStore, store3, names, databaseName);
                    }
                }
            }
            finally
            {
                // todo 
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
