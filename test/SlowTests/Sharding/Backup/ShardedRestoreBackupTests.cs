using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.Backups.Sharding;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Sharding;
using Raven.Server.Config;
using Raven.Server.Documents.PeriodicBackup.Aws;
using Raven.Server.Documents.PeriodicBackup.Azure;
using Raven.Server.Documents.PeriodicBackup.GoogleCloud;
using Raven.Server.ServerWide;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;
using BackupConfiguration = Raven.Server.Config.Categories.BackupConfiguration;

namespace SlowTests.Sharding.Backup
{
    public class ShardedRestoreBackupTests : ClusterTestBase
    {
        private readonly string _restoreFromS3TestsPrefix = $"sharding/tests/backup-restore/{nameof(ShardedRestoreBackupTests)}-{Guid.NewGuid()}";
        private readonly string _azureTestsPrefix = $"sharding/tests/backup-restore/{nameof(ShardedRestoreBackupTests)}-{Guid.NewGuid()}";
        private readonly string _googleCloudTestsPrefix = $"sharding/tests/backup-restore/{nameof(ShardedRestoreBackupTests)}-{Guid.NewGuid()}";
        private static readonly BackupConfiguration DefaultBackupConfiguration;

        static ShardedRestoreBackupTests()
        {
            var configuration = RavenConfiguration.CreateForTesting("foo", ResourceType.Database);
            configuration.Initialize();

            DefaultBackupConfiguration = configuration.Backup;
        }

        public ShardedRestoreBackupTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.BackupExportImport | RavenTestCategory.Sharding)]
        public async Task CanBackupAndRestoreShardedDatabaseInCluster()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");
            var cluster = await CreateRaftCluster(3, watcherCluster: true);

            var options = Sharding.GetOptionsForCluster(cluster.Leader, shards: 3, shardReplicationFactor: 1, orchestratorReplicationFactor: 3);
            using (var store = Sharding.GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < 10; i++)
                    {
                        session.Store(new User(), $"users/{i}");
                    }

                    session.SaveChanges();
                }

                var waitHandles = await Sharding.Backup.WaitForBackupsToComplete(cluster.Nodes, store.Database);

                var config = Backup.CreateBackupConfiguration(backupPath);
                await Sharding.Backup.UpdateConfigurationAndRunBackupAsync(cluster.Nodes, store, config);

                Assert.True(WaitHandle.WaitAll(waitHandles, TimeSpan.FromMinutes(1)));

                var dirs = Directory.GetDirectories(backupPath);
                Assert.Equal(cluster.Nodes.Count, dirs.Length);

                var sharding = await Sharding.GetShardingConfigurationAsync(store);
                var settings = GenerateShardRestoreSettings(dirs, sharding);

                // restore the database with a different name
                var newDbName = $"restored_database-{Guid.NewGuid()}";
                using (Sharding.Backup.ReadOnly(backupPath))
                using (Backup.RestoreDatabase(store, new RestoreBackupConfiguration
                {
                    DatabaseName = newDbName,
                    ShardRestoreSettings = settings
                }, timeout: TimeSpan.FromSeconds(60)))
                {
                    var dbRec = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(newDbName));
                    Assert.Equal(DatabaseStateStatus.Normal, dbRec.DatabaseState);
                    Assert.Equal(3, dbRec.Sharding.Shards.Length);

                    var shardNodes = new HashSet<string>();
                    for (var index = 0; index < dbRec.Sharding.Shards.Length; index++)
                    {
                        var shardTopology = dbRec.Sharding.Shards[index];
                        Assert.Equal(1, shardTopology.Members.Count);
                        Assert.Equal(sharding.Shards[index].Members[0], shardTopology.Members[0]);
                        Assert.True(shardNodes.Add(shardTopology.Members[0]));
                    }

                    using (var session = store.OpenSession(newDbName))
                    {
                        for (int i = 0; i < 10; i++)
                        {
                            var doc = session.Load<User>($"users/{i}");
                            Assert.NotNull(doc);
                        }
                    }
                }
            }
        }

        [RavenFact(RavenTestCategory.BackupExportImport | RavenTestCategory.Sharding)]
        public async Task CanBackupAndRestoreSharded_FromLocalBackup()
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

                var sharding = await Sharding.GetShardingConfigurationAsync(store1);
                var settings = GenerateShardRestoreSettings(dirs, sharding);

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

        [AmazonS3Fact]
        public async Task CanBackupAndRestoreSharded_FromS3Backup()
        {
            var s3Settings = GetS3Settings();
            try
            {
                var cluster = await CreateRaftCluster(3, watcherCluster: true);
                var options = Sharding.GetOptionsForCluster(cluster.Leader, shards: 3, shardReplicationFactor: 1, orchestratorReplicationFactor: 3);

                using (var store = Sharding.GetDocumentStore(options))
                {   
                    await Sharding.Backup.InsertData(store);

                    var waitHandles = await Sharding.Backup.WaitForBackupsToComplete(cluster.Nodes, store.Database);

                    var config = Backup.CreateBackupConfiguration(s3Settings: s3Settings);
                    await Sharding.Backup.UpdateConfigurationAndRunBackupAsync(cluster.Nodes, store, config);

                    Assert.True(WaitHandle.WaitAll(waitHandles, TimeSpan.FromMinutes(1)));

                    var sharding = await Sharding.GetShardingConfigurationAsync(store);
                    
                    ShardedRestoreSettings settings;
                    using (var s3Client = new RavenAwsS3Client(s3Settings, DefaultBackupConfiguration))
                    {
                        var prefix = $"{s3Settings.RemoteFolderName}/";
                        var cloudObjects = await s3Client.ListObjectsAsync(prefix, "/", listFolders: true);

                        Assert.Equal(3, cloudObjects.FileInfoDetails.Count);

                        settings = GenerateShardRestoreSettings(cloudObjects.FileInfoDetails.Select(fileInfo => fileInfo.FullPath).ToList(), sharding);
                    }

                    // restore the database with a different name
                    var databaseName = $"restored_database-{Guid.NewGuid()}";
                    using (Backup.RestoreDatabaseFromCloud(store, new RestoreFromS3Configuration
                    {
                        DatabaseName = databaseName,
                        ShardRestoreSettings = settings,
                        Settings = s3Settings
                    }, timeout: TimeSpan.FromSeconds(60)))
                    {
                        var dbRec = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(databaseName));
                        Assert.Equal(3, dbRec.Sharding.Shards.Length);

                        var shardNodes = new HashSet<string>();
                        for (var index = 0; index < dbRec.Sharding.Shards.Length; index++)
                        {
                            var shardTopology = dbRec.Sharding.Shards[index];
                            Assert.Equal(1, shardTopology.Members.Count);
                            Assert.Equal(sharding.Shards[index].Members[0], shardTopology.Members[0]);
                            Assert.True(shardNodes.Add(shardTopology.Members[0]));
                        }

                        await Sharding.Backup.CheckData(store, RavenDatabaseMode.Sharded, expectedRevisionsCount: 16, database: databaseName);
                    }
                }
            }
            finally
            {
                await DeleteObjects(s3Settings);
            }
        }

        [AmazonS3Fact]
        public async Task CanBackupAndRestoreSharded_FromAzureBackup()
        {
            var azureSettings = GetAzureSettings();
            try
            {
                var cluster = await CreateRaftCluster(3, watcherCluster: true);
                var options = Sharding.GetOptionsForCluster(cluster.Leader, shards: 3, shardReplicationFactor: 1, orchestratorReplicationFactor: 3);

                using (var store = Sharding.GetDocumentStore(options))
                {
                    await Sharding.Backup.InsertData(store);

                    var waitHandles = await Sharding.Backup.WaitForBackupsToComplete(cluster.Nodes, store.Database);

                    var config = Backup.CreateBackupConfiguration(azureSettings: azureSettings);
                    await Sharding.Backup.UpdateConfigurationAndRunBackupAsync(cluster.Nodes, store, config);

                    Assert.True(WaitHandle.WaitAll(waitHandles, TimeSpan.FromMinutes(1)));

                    var sharding = await Sharding.GetShardingConfigurationAsync(store);

                    ShardedRestoreSettings settings;
                    using (var client = RavenAzureClient.Create(azureSettings, DefaultBackupConfiguration))
                    {
                        var prefix = $"{azureSettings.RemoteFolderName}/";
                        var result = await client.ListBlobsAsync(prefix, delimiter: "/", listFolders: true);
                        var folderNames = result.List.Select(item => item.Name).ToList();
                        Assert.Equal(3, folderNames.Count);

                        settings = GenerateShardRestoreSettings(folderNames, sharding);
                    }

                    // restore the database with a different name
                    var databaseName = $"restored_database-{Guid.NewGuid()}";
                    using (Backup.RestoreDatabaseFromCloud(store, new RestoreFromAzureConfiguration
                    {
                        DatabaseName = databaseName,
                        ShardRestoreSettings = settings,
                        Settings = azureSettings
                    }, timeout: TimeSpan.FromSeconds(60)))
                    {
                        var dbRec = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(databaseName));
                        Assert.Equal(3, dbRec.Sharding.Shards.Length);

                        var shardNodes = new HashSet<string>();
                        for (var index = 0; index < dbRec.Sharding.Shards.Length; index++)
                        {
                            var shardTopology = dbRec.Sharding.Shards[index];
                            Assert.Equal(1, shardTopology.Members.Count);
                            Assert.Equal(sharding.Shards[index].Members[0], shardTopology.Members[0]);
                            Assert.True(shardNodes.Add(shardTopology.Members[0]));
                        }

                        await Sharding.Backup.CheckData(store, RavenDatabaseMode.Sharded, expectedRevisionsCount: 16, database: databaseName);
                    }
                }
            }
            finally
            {
                await DeleteObjects(azureSettings);
            }
        }

        [GoogleCloudFact]
        public async Task CanBackupAndRestoreSharded_FromGoogleCloudBackup()
        {
            var googleCloudSettings = GetGoogleCloudSettings();
            try
            {
                var cluster = await CreateRaftCluster(3, watcherCluster: true);
                var options = Sharding.GetOptionsForCluster(cluster.Leader, shards: 3, shardReplicationFactor: 1, orchestratorReplicationFactor: 3);

                using (var store = Sharding.GetDocumentStore(options))
                {
                    await Sharding.Backup.InsertData(store);

                    var waitHandles = await Sharding.Backup.WaitForBackupsToComplete(cluster.Nodes, store.Database);

                    var config = Backup.CreateBackupConfiguration(googleCloudSettings: googleCloudSettings);
                    await Sharding.Backup.UpdateConfigurationAndRunBackupAsync(cluster.Nodes, store, config);

                    Assert.True(WaitHandle.WaitAll(waitHandles, TimeSpan.FromMinutes(1)));

                    var sharding = await Sharding.GetShardingConfigurationAsync(store);

                    ShardedRestoreSettings settings;
                    using (var client = new RavenGoogleCloudClient(googleCloudSettings, DefaultBackupConfiguration))
                    {
                        var result = await client.ListObjectsAsync(googleCloudSettings.RemoteFolderName);
                        var fileNames = result.Select(item => item.Name).ToList();
                        Assert.Equal(3, fileNames.Count);

                        settings = GenerateShardRestoreSettings(fileNames, sharding);
                    }

                    // restore the database with a different name
                    var databaseName = $"restored_database-{Guid.NewGuid()}";
                    using (Backup.RestoreDatabaseFromCloud(store, new RestoreFromGoogleCloudConfiguration()
                    {
                        DatabaseName = databaseName,
                        ShardRestoreSettings = settings,
                        Settings = googleCloudSettings
                    }, timeout: TimeSpan.FromSeconds(60)))
                    {
                        var dbRec = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(databaseName));
                        Assert.Equal(3, dbRec.Sharding.Shards.Length);

                        var shardNodes = new HashSet<string>();
                        for (var index = 0; index < dbRec.Sharding.Shards.Length; index++)
                        {
                            var shardTopology = dbRec.Sharding.Shards[index];
                            Assert.Equal(1, shardTopology.Members.Count);
                            Assert.Equal(sharding.Shards[index].Members[0], shardTopology.Members[0]);
                            Assert.True(shardNodes.Add(shardTopology.Members[0]));
                        }

                        await Sharding.Backup.CheckData(store, RavenDatabaseMode.Sharded, expectedRevisionsCount: 16, database: databaseName);
                    }
                }
            }
            finally
            {
                await DeleteObjects(googleCloudSettings);
            }
        }

        private static ShardedRestoreSettings GenerateShardRestoreSettings(IReadOnlyCollection<string> backupPaths, ShardingConfiguration sharding)
        {
            var settings = new ShardedRestoreSettings
            {
                Shards = new SingleShardRestoreSetting[backupPaths.Count]
            };

            foreach (var dir in backupPaths)
            {
                var shardIndexPosition = dir.LastIndexOf('$') + 1;
                var shardNumber = int.Parse(dir[shardIndexPosition].ToString());

                settings.Shards[shardNumber] = new SingleShardRestoreSetting
                {
                    ShardNumber = shardNumber, 
                    FolderName = dir, 
                    NodeTag = sharding.Shards[shardNumber].Members[0]
                };
            }

            return settings;
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

        private AzureSettings GetAzureSettings([CallerMemberName] string caller = null)
        {
            var settings = AzureFactAttribute.AzureSettings;
            if (settings == null)
                return null;

            var remoteFolderName = _azureTestsPrefix;
            if (string.IsNullOrEmpty(caller) == false)
                remoteFolderName = $"{remoteFolderName}/{caller}";

            if (string.IsNullOrEmpty(settings.RemoteFolderName) == false)
                remoteFolderName = $"{settings.RemoteFolderName}/{remoteFolderName}";

            return new AzureSettings
            {
                RemoteFolderName = remoteFolderName,
                AccountName = settings.AccountName,
                StorageContainer = settings.StorageContainer,
                AccountKey = settings.AccountKey,
                SasToken = settings.SasToken
            };
        }

        private GoogleCloudSettings GetGoogleCloudSettings([CallerMemberName] string caller = null)
        {
            var googleCloudSettings = GoogleCloudFactAttribute.GoogleCloudSettings;
            if (googleCloudSettings == null)
                return null;

            var remoteFolderName = _googleCloudTestsPrefix;
            if (string.IsNullOrEmpty(caller) == false)
                remoteFolderName = $"{remoteFolderName}/{caller}";

            if (string.IsNullOrEmpty(googleCloudSettings.RemoteFolderName) == false)
                remoteFolderName = $"{googleCloudSettings.RemoteFolderName}/{remoteFolderName}";

            googleCloudSettings.RemoteFolderName = remoteFolderName;

            return googleCloudSettings;
        }

        private static async Task DeleteObjects(S3Settings s3Settings)
        {
            if (s3Settings == null)
                return;

            try
            {
                using (var s3Client = new RavenAwsS3Client(s3Settings, DefaultBackupConfiguration))
                {
                    var cloudObjects = await s3Client.ListObjectsAsync(prefix : $"{s3Settings.RemoteFolderName}/", delimiter : string.Empty, listFolders : false);
                    if (cloudObjects.FileInfoDetails.Count == 0)
                        return;

                    var pathsToDelete = cloudObjects.FileInfoDetails.Select(x => x.FullPath).ToList();
                    s3Client.DeleteMultipleObjects(pathsToDelete);
                }
            }
            catch (Exception)
            {
                // ignored
            }
        }

        private static async Task DeleteObjects(AzureSettings azureSettings)
        {
            if (azureSettings == null)
                return;

            try
            {
                using (var client = RavenAzureClient.Create(azureSettings, DefaultBackupConfiguration))
                {
                    var prefix = $"{azureSettings.RemoteFolderName}/";
                    var result = await client.ListBlobsAsync(prefix, delimiter: "/", listFolders: true);
                    var folderNames = result.List.Select(b => b.Name).ToList();

                    if (folderNames.Count == 0)
                        return;

                    var filesToDelete = new List<string>();
                    foreach (var folder in folderNames)
                    {
                        var objectsInFolder = await client.ListBlobsAsync(prefix: folder, delimiter: string.Empty, listFolders: false);
                        filesToDelete.AddRange(objectsInFolder.List.Select(b => b.Name));
                    }

                    filesToDelete.AddRange(folderNames);
                    filesToDelete.Add(prefix);

                    client.DeleteBlobs(filesToDelete);
                }
            }
            catch (Exception)
            {
                // ignored
            }
        }

        private static async Task DeleteObjects(GoogleCloudSettings settings)
        {
            if (settings == null)
                return;

            try
            {
                using (var client = new RavenGoogleCloudClient(settings, DefaultBackupConfiguration))
                {
                    var all = await client.ListObjectsAsync(prefix: settings.RemoteFolderName);
                    foreach (var obj in all)
                    {
                        await client.DeleteObjectAsync(obj.Name);
                    }
                }
            }
            catch (Exception)
            {
                // ignored
            }
        }

    }
}
