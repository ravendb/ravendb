using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.Backups.Sharding;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Operations.Configuration;
using Raven.Client.ServerWide.Sharding;
using Raven.Server.Config;
using Raven.Server.Documents.PeriodicBackup.Aws;
using Raven.Server.Documents.PeriodicBackup.Azure;
using Raven.Server.Documents.PeriodicBackup.GoogleCloud;
using Raven.Server.Documents.PeriodicBackup.Restore;
using Raven.Server.Documents.PeriodicBackup.Restore.Sharding;
using Raven.Server.Extensions;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Platform;
using Tests.Infrastructure;
using Tests.Infrastructure.Entities;
using Xunit;
using Xunit.Abstractions;
using BackupConfiguration = Raven.Server.Config.Categories.BackupConfiguration;

namespace SlowTests.Sharding.Backup
{
    public class ShardedRestoreBackupTests : ClusterTestBase
    {
        private readonly string _restoreFromS3TestsPrefix = $"sharding/tests/backup-restore/{nameof(ShardedRestoreBackupTests)}-{Guid.NewGuid()}";
        private readonly string _restoreFromAzureTestsPrefix = $"sharding/tests/backup-restore/{nameof(ShardedRestoreBackupTests)}-{Guid.NewGuid()}";
        private readonly string _restoreFromGoogleCloudTestsPrefix = $"sharding/tests/backup-restore/{nameof(ShardedRestoreBackupTests)}-{Guid.NewGuid()}";
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
        public async Task CanBackupAndRestoreShardedDatabase_InCluster()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");

            var cluster = await CreateRaftCluster(3, watcherCluster: true);

            var options = Sharding.GetOptionsForCluster(cluster.Leader, shards: 3, shardReplicationFactor: 1, orchestratorReplicationFactor: 3);
            using (var store = GetDocumentStore(options))
            {
                var record = await Sharding.GetShardingConfigurationAsync(store);
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
                var settings = Sharding.Backup.GenerateShardRestoreSettings(dirs, sharding);

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
                    Assert.Equal(3, dbRec.Sharding.Shards.Count);

                    var shardNodes = new HashSet<string>();
                    foreach (var shardToTopology in dbRec.Sharding.Shards)
                    {
                        Assert.Equal(1, shardToTopology.Value.Members.Count);
                        Assert.Equal(sharding.Shards[shardToTopology.Key].Members[0], shardToTopology.Value.Members[0]);
                        Assert.True(shardNodes.Add(shardToTopology.Value.Members[0]));
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
        public async Task CanBackupAndRestoreShardedDatabase_FromLocalBackup()
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
                var settings = Sharding.Backup.GenerateShardRestoreSettings(dirs, sharding);

                // restore the database with a different name
                var databaseName = $"restored_database-{Guid.NewGuid()}";
                using (Sharding.Backup.ReadOnly(backupPath))
                using (Backup.RestoreDatabase(store2, new RestoreBackupConfiguration
                {
                    DatabaseName = databaseName,
                    ShardRestoreSettings = settings

                }, timeout: TimeSpan.FromSeconds(60)))
                {
                    var databaseRecord = await store2.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(databaseName));
                    Assert.Equal(DatabaseStateStatus.Normal, databaseRecord.DatabaseState);
                    Assert.Equal(3, databaseRecord.Sharding.Shards.Count);
                    Assert.Equal(1, databaseRecord.PeriodicBackups.Count);
                    Assert.NotNull(databaseRecord.Revisions);

                    await Sharding.Backup.CheckData(store2, RavenDatabaseMode.Sharded, expectedRevisionsCount: 16, database: databaseName);
                }
            }
        }

        [RavenFact(RavenTestCategory.BackupExportImport | RavenTestCategory.Sharding)]
        public async Task EnsureValidationForRestoreFromShardedDatabaseConfiguration()
        {
            using (var store1 = Sharding.GetDocumentStore())
            using (var store2 = GetDocumentStore())
            {
                var dirs = new []{ "dir$0", "dir$1", "dir$2"};
                Assert.Equal(3, dirs.Length);

                var sharding = await Sharding.GetShardingConfigurationAsync(store1);
                var settings = Sharding.Backup.GenerateShardRestoreSettings(dirs, sharding);
                
                var databaseName = $"restored_database-{Guid.NewGuid()}";

                settings.Shards[0].NodeTag = null;
                var error = Assert.ThrowsAny<RavenException>(() =>
                {
                    Backup.RestoreDatabase(store2, new RestoreBackupConfiguration { DatabaseName = databaseName, ShardRestoreSettings = settings },
                        timeout: TimeSpan.FromSeconds(60));
                });
                Assert.Contains("was not provided a node tag", error.Message);
                settings.Shards[0].NodeTag = "A";

                settings.Shards[0].ShardNumber = 1;
                error = Assert.ThrowsAny<RavenException>(() =>
                {
                    Backup.RestoreDatabase(store2, new RestoreBackupConfiguration { DatabaseName = databaseName, ShardRestoreSettings = settings },
                        timeout: TimeSpan.FromSeconds(60));
                });
                Assert.Contains("there is a shard mismatch in the provided restore configuration", error.Message);
                settings.Shards[0].ShardNumber = 0;

                settings.Shards = null;
                error = Assert.ThrowsAny<RavenException>(() =>
                {
                    Backup.RestoreDatabase(store2, new RestoreBackupConfiguration { DatabaseName = databaseName, ShardRestoreSettings = settings },
                        timeout: TimeSpan.FromSeconds(60));
                });
                Assert.Contains($"configuration for field '{nameof(RestoreBackupConfiguration.ShardRestoreSettings.Shards)}' is not set", error.Message);
            }
        }

        [AmazonS3RetryFact]
        public async Task CanBackupAndRestoreShardedDatabase_FromS3Backup()
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

                        settings = Sharding.Backup.GenerateShardRestoreSettings(cloudObjects.FileInfoDetails.Select(fileInfo => fileInfo.FullPath).ToList(), sharding);
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
                        var databaseRecord = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(databaseName));

                        Assert.Equal(3, databaseRecord.Sharding.Shards.Count);
                        Assert.Equal(1, databaseRecord.PeriodicBackups.Count);
                        Assert.NotNull(databaseRecord.Revisions);

                        var shardNodes = new HashSet<string>();
                        foreach (var shardToTopology in databaseRecord.Sharding.Shards)
                        {
                            var shardTopology = shardToTopology.Value;
                            Assert.Equal(1, shardTopology.Members.Count);
                            Assert.Equal(sharding.Shards[shardToTopology.Key].Members[0], shardTopology.Members[0]);
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

        [AzureRetryFact]
        public async Task CanBackupAndRestoreShardedDatabase_FromAzureBackup()
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

                        settings = Sharding.Backup.GenerateShardRestoreSettings(folderNames, sharding);
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
                        Assert.Equal(3, dbRec.Sharding.Shards.Count);

                        var shardNodes = new HashSet<string>();
                        foreach (var shardToTopology in dbRec.Sharding.Shards)
                        {
                            var shardTopology = shardToTopology.Value;
                            Assert.Equal(1, shardTopology.Members.Count);
                            Assert.Equal(sharding.Shards[shardToTopology.Key].Members[0], shardTopology.Members[0]);
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

        [GoogleCloudRetryFact]
        public async Task CanBackupAndRestoreShardedDatabase_FromGoogleCloudBackup()
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

                        settings = Sharding.Backup.GenerateShardRestoreSettings(fileNames, sharding);
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
                        Assert.Equal(3, dbRec.Sharding.Shards.Count);

                        var shardNodes = new HashSet<string>();
                        foreach (var shardToTopology in dbRec.Sharding.Shards)
                        {
                            var shardTopology = shardToTopology.Value;
                            Assert.Equal(1, shardTopology.Members.Count);
                            Assert.Equal(sharding.Shards[shardToTopology.Key].Members[0], shardTopology.Members[0]);
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

        [RavenFact(RavenTestCategory.BackupExportImport | RavenTestCategory.Sharding)]
        public async Task CanBackupAndRestoreShardedDatabase_IncrementalBackup()
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
                var backupTaskId = await Sharding.Backup.UpdateConfigurationAndRunBackupAsync(cluster.Nodes, store, config, isFullBackup: false);

                Assert.True(WaitHandle.WaitAll(waitHandles, TimeSpan.FromMinutes(1)));

                // add more data
                waitHandles = await Sharding.Backup.WaitForBackupsToComplete(cluster.Nodes, store.Database);
                using (var session = store.OpenAsyncSession())
                {
                    for (int i = 0; i < 10; i++)
                    {
                        session.CountersFor($"users/{i}").Increment("downloads", i);
                    }

                    await session.StoreAsync(new User { Name = "ayende" }, "users/11");
                    await session.SaveChangesAsync();
                }

                await Sharding.Backup.RunBackupAsync(store, backupTaskId, isFullBackup: false);
                Assert.True(WaitHandle.WaitAll(waitHandles, TimeSpan.FromMinutes(1)));

                var dirs = Directory.GetDirectories(backupPath);
                Assert.Equal(cluster.Nodes.Count, dirs.Length);

                foreach (var dir in dirs)
                {
                    var files = Directory.GetFiles(dir);
                    Assert.Equal(2, files.Length);
                }

                var sharding = await Sharding.GetShardingConfigurationAsync(store);
                var settings = Sharding.Backup.GenerateShardRestoreSettings(dirs, sharding);

                // restore the database with a different name
                var databaseName = $"restored_database-{Guid.NewGuid()}";
                using (Sharding.Backup.ReadOnly(backupPath))
                using (Backup.RestoreDatabase(store, new RestoreBackupConfiguration
                {
                    DatabaseName = databaseName,
                    ShardRestoreSettings = settings
                }, timeout: TimeSpan.FromSeconds(60)))
                {
                    var dbRec = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(databaseName));
                    Assert.Equal(3, dbRec.Sharding.Shards.Count);

                    var shardNodes = new HashSet<string>();
                    foreach (var shardToTopology in dbRec.Sharding.Shards)
                    {
                        var shardTopology = shardToTopology.Value;
                        Assert.Equal(1, shardTopology.Members.Count);
                        Assert.Equal(sharding.Shards[shardToTopology.Key].Members[0], shardTopology.Members[0]);
                        Assert.True(shardNodes.Add(shardTopology.Members[0]));
                    }

                    using (var session = store.OpenSession(databaseName))
                    {
                        for (int i = 0; i < 10; i++)
                        {
                            var doc = session.Load<User>($"users/{i}");
                            Assert.NotNull(doc);

                            var counter = session.CountersFor(doc).Get("downloads");
                            Assert.Equal(i, counter);
                        }

                        var user = session.Load<User>("users/11");
                        Assert.Equal("ayende", user.Name);
                    }
                }
            }
        }

        [RavenFact(RavenTestCategory.BackupExportImport | RavenTestCategory.Sharding | RavenTestCategory.Encryption)]
        public async Task EncryptedBackupAndRestoreSharded_UsingProvidedKey()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");

            var cluster = await CreateRaftCluster(3, watcherCluster: true);
            var options = Sharding.GetOptionsForCluster(cluster.Leader, shards: 3, shardReplicationFactor: 1, orchestratorReplicationFactor: 3);

            using (var store = Sharding.GetDocumentStore(options))
            {
                await Sharding.Backup.InsertData(store);

                var waitHandles = await Sharding.Backup.WaitForBackupsToComplete(cluster.Nodes, store.Database);

                var config = Backup.CreateBackupConfiguration(backupPath, backupEncryptionSettings: new BackupEncryptionSettings
                {
                    Key = "OI7Vll7DroXdUORtc6Uo64wdAk1W0Db9ExXXgcg5IUs=",
                    EncryptionMode = EncryptionMode.UseProvidedKey
                });

                await Sharding.Backup.UpdateConfigurationAndRunBackupAsync(cluster.Nodes, store, config);

                Assert.True(WaitHandle.WaitAll(waitHandles, TimeSpan.FromMinutes(1)));

                var dirs = Directory.GetDirectories(backupPath);
                Assert.Equal(cluster.Nodes.Count, dirs.Length);

                var sharding = await Sharding.GetShardingConfigurationAsync(store);
                var settings = Sharding.Backup.GenerateShardRestoreSettings(dirs, sharding);

                // restore the database with a different name
                var databaseName = $"restored_database-{Guid.NewGuid()}";
                using (Sharding.Backup.ReadOnly(backupPath))
                using (Backup.RestoreDatabase(store, new RestoreBackupConfiguration
                {
                    DatabaseName = databaseName,
                    ShardRestoreSettings = settings,
                    BackupEncryptionSettings = new BackupEncryptionSettings
                    {
                        Key = "OI7Vll7DroXdUORtc6Uo64wdAk1W0Db9ExXXgcg5IUs=",
                        EncryptionMode = EncryptionMode.UseProvidedKey
                    }
                }, timeout: TimeSpan.FromSeconds(60)))
                {
                    var dbRec = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(databaseName));
                    Assert.Equal(3, dbRec.Sharding.Shards.Count);

                    var shardNodes = new HashSet<string>();
                    foreach (var shardToTopology in dbRec.Sharding.Shards)
                    {
                        var shardTopology = shardToTopology.Value;
                        Assert.Equal(1, shardTopology.Members.Count);
                        Assert.Equal(sharding.Shards[shardToTopology.Key].Members[0], shardTopology.Members[0]);
                        Assert.True(shardNodes.Add(shardTopology.Members[0]));
                    }

                    await Sharding.Backup.CheckData(store, RavenDatabaseMode.Sharded, expectedRevisionsCount: 16, database: databaseName);
                }
            }
        }

        [AmazonS3RetryFact]
        public async Task EncryptedBackupAndRestoreShardedDatabase_UsingDatabaseKey()
        {
            var s3Settings = GetS3Settings();
         
            try
            {
                var key = Encryption.EncryptedServer(out var certificates, out var dbName);

                using (var store = Sharding.GetDocumentStore(new Options
                {
                   AdminCertificate = certificates.ServerCertificate.Value,
                   ClientCertificate = certificates.ServerCertificate.Value,
                   ModifyDatabaseName = s => dbName,
                   ModifyDatabaseRecord = record => record.Encrypted = true
                }))
                {
                    using (var session = store.OpenSession())
                    {
                        for (int i = 0; i < 10; i++)
                        {
                            session.Store(new User(), $"users/{i}");
                        }
                        session.SaveChanges();
                    }

                    var waitHandles = await Sharding.Backup.WaitForBackupToComplete(store);

                    var config = Backup.CreateBackupConfiguration(s3Settings: s3Settings, fullBackupFrequency: null, incrementalBackupFrequency: "0 */6 * * *", 
                        backupEncryptionSettings: new BackupEncryptionSettings
                        {
                            EncryptionMode = EncryptionMode.UseDatabaseKey
                        });

                    var backupTaskId = await Sharding.Backup.UpdateConfigurationAndRunBackupAsync(Server, store, config);

                    Assert.True(WaitHandle.WaitAll(waitHandles, TimeSpan.FromMinutes(1)));

                    // add more data
                    waitHandles = await Sharding.Backup.WaitForBackupToComplete(store);
                    using (var session = store.OpenAsyncSession())
                    {
                        for (int i = 0; i < 10; i++)
                        {
                            session.CountersFor($"users/{i}").Increment("downloads", i);
                        }

                        await session.StoreAsync(new User { Name = "ayende" }, "users/11");
                        await session.SaveChangesAsync();
                    }

                    await Sharding.Backup.RunBackupAsync(store, backupTaskId, false);
                    Assert.True(WaitHandle.WaitAll(waitHandles, TimeSpan.FromMinutes(1)));

                    var sharding = await Sharding.GetShardingConfigurationAsync(store);

                    ShardedRestoreSettings shardedRestoreSettings;
                    using (var s3Client = new RavenAwsS3Client(s3Settings, DefaultBackupConfiguration))
                    {
                        var prefix = $"{s3Settings.RemoteFolderName}/";
                        var cloudObjects = await s3Client.ListObjectsAsync(prefix, "/", listFolders: true);

                        Assert.Equal(3, cloudObjects.FileInfoDetails.Count);

                        var folderNames = cloudObjects.FileInfoDetails.Select(fileInfo => fileInfo.FullPath).ToList();

                        foreach (var folderName in folderNames)
                        {
                            var files = await s3Client.ListObjectsAsync(folderName, string.Empty, listFolders: false);
                            Assert.Equal(2, files.FileInfoDetails.Count);
                        }

                        shardedRestoreSettings = Sharding.Backup.GenerateShardRestoreSettings(folderNames, sharding);
                    }

                    var databaseName = $"restored_database-{Guid.NewGuid()}";
                    using (Backup.RestoreDatabaseFromCloud(store, new RestoreFromS3Configuration
                    {
                       Settings = s3Settings,
                       ShardRestoreSettings = shardedRestoreSettings,
                       DatabaseName = databaseName,
                       EncryptionKey = key,
                       BackupEncryptionSettings = new BackupEncryptionSettings
                       {
                           EncryptionMode = EncryptionMode.UseDatabaseKey
                       }
                    }))
                    {
                        var dbRec = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(databaseName));
                        Assert.Equal(3, dbRec.Sharding.Shards.Count);
                        Assert.True(dbRec.Encrypted);

                        foreach (var shardToTopology in dbRec.Sharding.Shards)
                        {
                            var shardTopology = shardToTopology.Value;
                            Assert.Equal(1, shardTopology.Members.Count);
                            Assert.Equal(sharding.Shards[shardToTopology.Key].Members[0], shardTopology.Members[0]);
                        }

                        using (var session = store.OpenSession(databaseName))
                        {
                            for (int i = 0; i < 10; i++)
                            {
                                var doc = session.Load<User>($"users/{i}");
                                Assert.NotNull(doc);

                                var counter = session.CountersFor(doc).Get("downloads");
                                Assert.Equal(i, counter);
                            }

                            var user = session.Load<User>("users/11");
                            Assert.Equal("ayende", user.Name);
                        }
                    }
                }
            }
            finally
            {
                await DeleteObjects(s3Settings);
            }
        }

        [AmazonS3RetryFact]
        public async Task EncryptedBackupAndRestoreShardedDatabaseInCluster_UsingDatabaseKey()
        {
            var s3Settings = GetS3Settings();
            try
            {
                var (nodes, leader, certificates) = await CreateRaftClusterWithSsl(3, watcherCluster: true);
                var key = Encryption.SetupEncryptedDatabaseInCluster(nodes, certificates, out var databaseName);

                var options = Sharding.GetOptionsForCluster(leader, shards: 3, shardReplicationFactor: 1, orchestratorReplicationFactor: 3);
                options.ClientCertificate = certificates.ClientCertificate1.Value;
                options.AdminCertificate = certificates.ServerCertificate.Value;
                options.ModifyDatabaseName = _ => databaseName;
                options.ModifyDatabaseRecord += record => record.Encrypted = true;
                options.RunInMemory = false;

                using (var store = Sharding.GetDocumentStore(options))
                {
                    await Sharding.Backup.InsertData(store);

                    var waitHandles = await Sharding.Backup.WaitForBackupsToComplete(nodes, store.Database);

                    var config = Backup.CreateBackupConfiguration(s3Settings: s3Settings,
                        backupEncryptionSettings: new BackupEncryptionSettings
                        {
                            EncryptionMode = EncryptionMode.UseDatabaseKey
                        });

                    await Sharding.Backup.UpdateConfigurationAndRunBackupAsync(nodes, store, config);

                    Assert.True(WaitHandle.WaitAll(waitHandles, TimeSpan.FromMinutes(1)));

                    var sharding = await Sharding.GetShardingConfigurationAsync(store);

                    ShardedRestoreSettings shardedRestoreSettings;
                    using (var s3Client = new RavenAwsS3Client(s3Settings, DefaultBackupConfiguration))
                    {
                        var prefix = $"{s3Settings.RemoteFolderName}/";
                        var cloudObjects = await s3Client.ListObjectsAsync(prefix, "/", listFolders: true);

                        Assert.Equal(3, cloudObjects.FileInfoDetails.Count);

                        var folderNames = cloudObjects.FileInfoDetails.Select(fileInfo => fileInfo.FullPath).ToList();
                        shardedRestoreSettings = Sharding.Backup.GenerateShardRestoreSettings(folderNames, sharding);
                    }

                    var newDbName = $"restored_database-{Guid.NewGuid()}";
                    using (Backup.RestoreDatabaseFromCloud(store, new RestoreFromS3Configuration
                    {
                        Settings = s3Settings,
                        ShardRestoreSettings = shardedRestoreSettings,
                        DatabaseName = newDbName,
                        EncryptionKey = key,
                        BackupEncryptionSettings = new BackupEncryptionSettings
                        {
                            EncryptionMode = EncryptionMode.UseDatabaseKey
                        }
                    }))
                    {
                        var dbRec = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(newDbName));
                        Assert.Equal(3, dbRec.Sharding.Shards.Count);
                        Assert.True(dbRec.Encrypted);

                        var shardNodes = new HashSet<string>();
                        foreach (var shardToTopology in dbRec.Sharding.Shards)
                        {
                            var shardTopology = shardToTopology.Value;
                            Assert.Equal(1, shardTopology.Members.Count);
                            Assert.Equal(sharding.Shards[shardToTopology.Key].Members[0], shardTopology.Members[0]);
                            Assert.True(shardNodes.Add(shardTopology.Members[0]));
                        }

                        await Sharding.Backup.CheckData(store, RavenDatabaseMode.Sharded, expectedRevisionsCount: 16, newDbName);
                    }
                }
            }
            finally
            {
                await DeleteObjects(s3Settings);
            }
        }

        [RavenFact(RavenTestCategory.BackupExportImport | RavenTestCategory.Sharding)]
        public async Task RestoreShardedDatabase_ShouldHaveValidResultsAndProgress()
        {
            var cluster = await CreateRaftCluster(3, watcherCluster: true);
            var options = Sharding.GetOptionsForCluster(cluster.Leader, shards: 3, shardReplicationFactor: 1, orchestratorReplicationFactor: 3);

            using (var store = Sharding.GetDocumentStore(options))
            {
                Cluster.WaitForFirstCompareExchangeTombstonesClean(cluster.Leader);

                await Sharding.Backup.InsertData(store);

                var waitHandles = await Sharding.Backup.WaitForBackupsToComplete(cluster.Nodes, store.Database);

                var backupPath = NewDataPath(suffix: "BackupFolder");

                var config = Backup.CreateBackupConfiguration(backupPath);
                await Sharding.Backup.UpdateConfigurationAndRunBackupAsync(cluster.Nodes, store, config);

                Assert.True(WaitHandle.WaitAll(waitHandles, TimeSpan.FromMinutes(1)));

                var dirs = Directory.GetDirectories(backupPath);
                Assert.Equal(3, dirs.Length);

                var sharding = await Sharding.GetShardingConfigurationAsync(store);
                var settings = Sharding.Backup.GenerateShardRestoreSettings(dirs, sharding);

                // restore the database with a different name
                var databaseName = $"restored_database-{Guid.NewGuid()}";
                using (Sharding.Backup.ReadOnly(backupPath))
                using (Databases.EnsureDatabaseDeletion(databaseName, store))
                {
                    var restoreOperation = new RestoreBackupOperation(new RestoreBackupConfiguration
                    {
                        DatabaseName = databaseName,
                        ShardRestoreSettings = settings
                    });

                    var operation = await store.Maintenance.Server.SendAsync(restoreOperation);

                    var progresses = new List<IOperationProgress>();

                    operation.OnProgressChanged += (_, progress) =>
                    {
                        progresses.Add(progress);
                    };

                    var result = await operation.WaitForCompletionAsync(TimeSpan.FromSeconds(60));

                    Assert.NotEmpty(progresses);

                    ValidateRestoreResult(result, sharding);

                    var databaseRecord = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(databaseName));
                    Assert.Equal(3, databaseRecord.Sharding.Shards.Count);
                    Assert.Equal(1, databaseRecord.PeriodicBackups.Count);
                    Assert.NotNull(databaseRecord.Revisions);

                    var shardNodes = new HashSet<string>();
                    var expectedShardNumber = 0;
                    foreach (var shardToTopology in databaseRecord.Sharding.Shards)
                    {
                        var shardTopology = shardToTopology.Value;
                        var shardNumber = shardToTopology.Key;

                        Assert.Equal(1, shardTopology.Members.Count);
                        Assert.Equal(sharding.Shards[shardNumber].Members[0], shardTopology.Members[0]);
                        Assert.True(shardNodes.Add(shardTopology.Members[0]));

                        Assert.Equal(expectedShardNumber++, shardNumber);
                    }

                    for (int i = 0; i < databaseRecord.Sharding.BucketRanges.Count; i++)
                    {
                        var shardBucketRange = databaseRecord.Sharding.BucketRanges[i];
                        Assert.Equal(i, shardBucketRange.ShardNumber);
                    }

                    await Sharding.Backup.CheckData(store, RavenDatabaseMode.Sharded, expectedRevisionsCount: 16, database: databaseName);
                }
            }
        }

        [RavenFact(RavenTestCategory.BackupExportImport | RavenTestCategory.Sharding)]
        public async Task BackupAndRestoreShardedDatabase_ShouldPreserveBucketRanges()
        {
            using (var store = Sharding.GetDocumentStore())
            {
                const string id = "users/1/$b";
                var originalLocation = await Sharding.GetShardNumberForAsync(store, id);
                Assert.Equal(0, originalLocation);

                var databaseRecord = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));

                // change shard-0 bucket ranges to [0 , 100] 
                databaseRecord.Sharding.BucketRanges[1].BucketRangeStart = 100;

                store.Maintenance.Server.Send(new UpdateDatabaseOperation(databaseRecord, replicationFactor: 1, databaseRecord.Etag));

                var newLocation = await Sharding.GetShardNumberForAsync(store, id);
                Assert.NotEqual(originalLocation, newLocation);

                using (var session = store.OpenAsyncSession())
                {
                    for (int i = 0; i < 10; i++)
                    {
                        await session.StoreAsync(new User(), $"users/{i}/$b");
                    }

                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession(database: ShardHelper.ToShardName(store.Database, originalLocation)))
                {
                    for (int i = 0; i < 10; i++)
                    {
                        var doc = await session.LoadAsync<User>($"users/{i}/$b");
                        Assert.Null(doc);
                    }
                }

                using (var session = store.OpenAsyncSession(database: ShardHelper.ToShardName(store.Database, newLocation)))
                {
                    for (int i = 0; i < 10; i++)
                    {
                        var doc = await session.LoadAsync<User>($"users/{i}/$b");
                        Assert.NotNull(doc);
                    }
                }

                var waitHandles = await Sharding.Backup.WaitForBackupToComplete(store);
                var backupPath = NewDataPath(suffix: "BackupFolder");
                var config = Backup.CreateBackupConfiguration(backupPath);

                await Sharding.Backup.UpdateConfigurationAndRunBackupAsync(Server, store, config);
                Assert.True(WaitHandle.WaitAll(waitHandles, TimeSpan.FromMinutes(1)));

                var dirs = Directory.GetDirectories(backupPath);
                Assert.Equal(3, dirs.Length);

                var sharding = await Sharding.GetShardingConfigurationAsync(store);
                var settings = Sharding.Backup.GenerateShardRestoreSettings(dirs, sharding);

                // restore the database with a different name
                var restoredDatabaseName = $"restored_database-{Guid.NewGuid()}";
                using (Sharding.Backup.ReadOnly(backupPath))
                using (Backup.RestoreDatabase(store, new RestoreBackupConfiguration
                {
                    DatabaseName = restoredDatabaseName,
                    ShardRestoreSettings = settings

                }, timeout: TimeSpan.FromSeconds(60)))
                {
                    var newDatabaseRecord = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(restoredDatabaseName));
                    Assert.Equal(3, newDatabaseRecord.Sharding.Shards.Count);
                    Assert.Equal(100, newDatabaseRecord.Sharding.BucketRanges[1].BucketRangeStart);

                    using (var session = store.OpenAsyncSession(database: restoredDatabaseName))
                    {
                        // should go to 'newLocation' shard
                        var newDocId = "users/10/$b";
                        await session.StoreAsync(new User(), newDocId);
                        await session.SaveChangesAsync();
                    }

                    using (var session = store.OpenAsyncSession(database: restoredDatabaseName))
                    {
                        for (int i = 0; i <= 10; i++)
                        {
                            var doc = await session.LoadAsync<User>($"users/{i}/$b");
                            Assert.NotNull(doc);
                        }
                    }

                    using (var session = store.OpenAsyncSession(database: ShardHelper.ToShardName(restoredDatabaseName, originalLocation)))
                    {
                        for (int i = 0; i <= 10; i++)
                        {
                            var doc = await session.LoadAsync<User>($"users/{i}/$b");
                            Assert.Null(doc);
                        }
                    }

                    using (var session = store.OpenAsyncSession(database: ShardHelper.ToShardName(restoredDatabaseName, newLocation)))
                    {
                        for (int i = 0; i <= 10; i++)
                        {
                            var doc = await session.LoadAsync<User>($"users/{i}/$b");
                            Assert.NotNull(doc);
                        }
                    }
                }
            }
        }

        private static void ValidateRestoreResult(IOperationResult result, ShardingConfiguration sharding)
        {
            var shardedRestoreResults = ((ShardedRestoreResult)result).Results.OrderBy(r => r.ShardNumber).ToList();
            Assert.Equal(3, shardedRestoreResults.Count);

            Assert.Equal(2, shardedRestoreResults[0].Result.Documents.ReadCount);
            Assert.Equal(2, shardedRestoreResults[0].Result.Documents.Attachments.ReadCount);
            Assert.Equal(1, shardedRestoreResults[0].Result.CompareExchange.ReadCount);
            Assert.Equal(1, shardedRestoreResults[0].Result.CompareExchangeTombstones.ReadCount);
            Assert.Equal(1, shardedRestoreResults[0].Result.Counters.ReadCount);
            Assert.Equal(1, shardedRestoreResults[0].Result.TimeSeries.ReadCount);
            Assert.Equal(6, shardedRestoreResults[0].Result.RevisionDocuments.ReadCount);
            Assert.Equal(2, shardedRestoreResults[0].Result.RevisionDocuments.Attachments.ReadCount);
            Assert.Equal(0, shardedRestoreResults[0].Result.Tombstones.ReadCount);
            Assert.Equal(1, shardedRestoreResults[0].Result.Identities.ReadCount);
            Assert.Equal(1, shardedRestoreResults[0].Result.Indexes.ReadCount);
            Assert.Equal(1, shardedRestoreResults[0].Result.Subscriptions.ReadCount);

            Assert.Equal(sharding.Shards[0].Members[0], shardedRestoreResults[0].NodeTag);

            Assert.Equal(0, shardedRestoreResults[1].Result.Documents.ReadCount);
            Assert.Equal(0, shardedRestoreResults[1].Result.Documents.Attachments.ReadCount);
            Assert.Equal(1, shardedRestoreResults[1].Result.CompareExchange.ReadCount);
            Assert.Equal(1, shardedRestoreResults[1].Result.CompareExchangeTombstones.ReadCount);
            Assert.Equal(0, shardedRestoreResults[1].Result.Counters.ReadCount);
            Assert.Equal(0, shardedRestoreResults[1].Result.TimeSeries.ReadCount);
            Assert.Equal(2, shardedRestoreResults[1].Result.RevisionDocuments.ReadCount);
            Assert.Equal(0, shardedRestoreResults[1].Result.RevisionDocuments.Attachments.ReadCount);
            Assert.Equal(1, shardedRestoreResults[1].Result.Tombstones.ReadCount);
            Assert.Equal(1, shardedRestoreResults[1].Result.Identities.ReadCount);
            Assert.Equal(1, shardedRestoreResults[1].Result.Indexes.ReadCount);
            Assert.Equal(0, shardedRestoreResults[1].Result.Subscriptions.ReadCount);

            Assert.Equal(sharding.Shards[1].Members[0], shardedRestoreResults[1].NodeTag);

            Assert.Equal(3, shardedRestoreResults[2].Result.Documents.ReadCount);
            Assert.Equal(1, shardedRestoreResults[2].Result.Documents.Attachments.ReadCount);
            Assert.Equal(3, shardedRestoreResults[2].Result.CompareExchange.ReadCount);
            Assert.Equal(1, shardedRestoreResults[2].Result.CompareExchangeTombstones.ReadCount);
            Assert.Equal(0, shardedRestoreResults[2].Result.Counters.ReadCount);
            Assert.Equal(1, shardedRestoreResults[2].Result.TimeSeries.ReadCount);
            Assert.Equal(6, shardedRestoreResults[2].Result.RevisionDocuments.ReadCount);
            Assert.Equal(1, shardedRestoreResults[2].Result.RevisionDocuments.Attachments.ReadCount);
            Assert.Equal(0, shardedRestoreResults[2].Result.Tombstones.ReadCount);
            Assert.Equal(1, shardedRestoreResults[2].Result.Identities.ReadCount);
            Assert.Equal(1, shardedRestoreResults[2].Result.Indexes.ReadCount);
            Assert.Equal(0, shardedRestoreResults[2].Result.Subscriptions.ReadCount);

            Assert.Equal(sharding.Shards[2].Members[0], shardedRestoreResults[2].NodeTag);
        }

        [RavenFact(RavenTestCategory.BackupExportImport | RavenTestCategory.Sharding)]
        public async Task CanBackupAndRestoreShardedDatabase_WithAtomicGuardTombstones()
        {
            //RavenDB-19201
            using (var store = Sharding.GetDocumentStore())
            {
                Cluster.WaitForFirstCompareExchangeTombstonesClean(Server);

                using (var session = store.OpenAsyncSession())
                {
                    for (int i = 0; i < 10; i++)
                    {
                        await session.StoreAsync(new User(), $"users/{i}");
                    }

                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession(new SessionOptions
                {
                    TransactionMode = TransactionMode.ClusterWide
                }))
                {
                    var user = new User { Name = "Ayende" };
                    await session.StoreAsync(user, "users/ayende");
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession(new SessionOptions
                {
                    TransactionMode = TransactionMode.ClusterWide
                }))
                {
                    session.Delete("users/ayende");
                    await session.SaveChangesAsync();
                }

                using (Server.ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
                using (context.OpenReadTransaction())
                {
#pragma warning disable CS0618
                    var compareExchangeTombs = Server.ServerStore.Cluster.GetCompareExchangeTombstonesByKey(context, store.Database).ToList();
#pragma warning restore CS0618
                    Assert.Equal(1, compareExchangeTombs.Count);
                    Assert.Equal("rvn-atomic/users/ayende", compareExchangeTombs[0].Key.Key);
                }

                var waitHandles = await Sharding.Backup.WaitForBackupToComplete(store);

                var backupPath = NewDataPath(suffix: "BackupFolder");

                var config = Backup.CreateBackupConfiguration(backupPath);
                await Sharding.Backup.UpdateConfigurationAndRunBackupAsync(Server, store, config);

                Assert.True(WaitHandle.WaitAll(waitHandles, TimeSpan.FromMinutes(1)));

                var dirs = Directory.GetDirectories(backupPath);
                Assert.Equal(3, dirs.Length);

                var sharding = await Sharding.GetShardingConfigurationAsync(store);
                var settings = Sharding.Backup.GenerateShardRestoreSettings(dirs, sharding);

                // restore the database with a different name
                var databaseName = $"restored_database-{Guid.NewGuid()}";

                var restoreOperation = new RestoreBackupOperation(new RestoreBackupConfiguration
                {
                    DatabaseName = databaseName,
                    ShardRestoreSettings = settings

                });

                using (Sharding.Backup.ReadOnly(backupPath))
                using (Databases.EnsureDatabaseDeletion(databaseName, store))
                {
                    var operation = await store.Maintenance.Server.SendAsync(restoreOperation);
                    var result = await operation.WaitForCompletionAsync(TimeSpan.FromSeconds(20)) as ShardedRestoreResult;
                    Assert.NotNull(result);

                    var compareExchangeTombsReadCount = result.Results.Sum(r => r.Result.CompareExchangeTombstones.ReadCount);
                    var docsReadCount = result.Results.Sum(r => r.Result.Documents.ReadCount);

                    Assert.Equal(1, compareExchangeTombsReadCount);
                    Assert.Equal(10, docsReadCount);
                }
            }
        }

        [RavenFact(RavenTestCategory.BackupExportImport | RavenTestCategory.Sharding)]
        public async Task OnFailureToRestore_ShouldDeleteEntireShardedDatabaseFromAllNodes()
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
                Assert.Equal(3, dirs.Length);

                var sharding = await Sharding.GetShardingConfigurationAsync(store);
                var settings = Sharding.Backup.GenerateShardRestoreSettings(dirs, sharding);

                // delete backup file of shard 1 in order to cause restore failure on one of the shards
                var shard1BackupDir = settings.Shards[1].FolderName;
                var backupFile = Directory.GetFiles(shard1BackupDir).Single();
                File.Delete(backupFile);

                var restoredDatabaseName = $"restored_database-{Guid.NewGuid()}";
                var restoreConfiguration = new RestoreBackupConfiguration
                {
                    DatabaseName = restoredDatabaseName,
                    ShardRestoreSettings = settings
                };

                Assert.Throws<RavenException>(() =>
                    Backup.RestoreDatabase(store, restoreConfiguration, timeout: TimeSpan.FromSeconds(60)));

                await foreach (var database in Sharding.GetShardsDocumentDatabaseInstancesFor(store.Database, cluster.Nodes))
                {
                    using (database.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                    using (context.OpenReadTransaction())
                    {
                        var databaseExists = database.ServerStore.Cluster.DatabaseExists(context, restoredDatabaseName);
                        Assert.False(databaseExists);
                    }
                }
            }
        }

        [RavenFact(RavenTestCategory.BackupExportImport | RavenTestCategory.Sharding)]
        public async Task CanRestoreShardedDatabase_UsingRestorePoint_FromLocalBackup()
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
                var backupTaskId = await Sharding.Backup.UpdateConfigurationAndRunBackupAsync(cluster.Nodes, store, config, isFullBackup: false);

                Assert.True(WaitHandle.WaitAll(waitHandles, TimeSpan.FromMinutes(1)));

                // add counters
                waitHandles = await Sharding.Backup.WaitForBackupsToComplete(cluster.Nodes, store.Database);
                using (var session = store.OpenAsyncSession())
                {
                    for (int i = 0; i < 10; i++)
                    {
                        session.CountersFor($"users/{i}").Increment("downloads", i);
                    }

                    await session.SaveChangesAsync();
                }

                await Sharding.Backup.RunBackupAsync(store, backupTaskId, isFullBackup: false);
                Assert.True(WaitHandle.WaitAll(waitHandles, TimeSpan.FromMinutes(1)));

                // add more docs
                waitHandles = await Sharding.Backup.WaitForBackupsToComplete(cluster.Nodes, store.Database);
                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < 10; i++)
                    {
                        session.Store(new Order(), $"orders/{i}");
                    }

                    session.SaveChanges();
                }

                await Sharding.Backup.RunBackupAsync(store, backupTaskId, isFullBackup: false);
                Assert.True(WaitHandle.WaitAll(waitHandles, TimeSpan.FromMinutes(1)));

                var dirs = Directory.GetDirectories(backupPath);
                Assert.Equal(cluster.Nodes.Count, dirs.Length);

                foreach (var dir in dirs)
                {
                    var files = Directory.GetFiles(dir);
                    Assert.Equal(3, files.Length);
                }

                var client = store.GetRequestExecutor().HttpClient;
                var data = new StringContent(JsonConvert.SerializeObject(new LocalSettings
                {
                    FolderPath = backupPath
                }), Encoding.UTF8, "application/json");
                var response = await client.SendAsync(new HttpRequestMessage(HttpMethod.Post, $"{store.Urls.First()}/admin/restore/points?type=Local")
                {
                    Content = data
                }.WithConventions(store.Conventions));
                string result = await response.Content.ReadAsStringAsync();

                var restorePoints = JsonConvert.DeserializeObject<RestorePoints>(result);
                Assert.Equal(9, restorePoints.List.Count);

                var pointsPerShard = restorePoints.List.GroupBy(p => p.DatabaseName).ToList();
                Assert.Equal(3, pointsPerShard.Count);

                var sharding = await Sharding.GetShardingConfigurationAsync(store);
                var settings = Sharding.Backup.GenerateShardRestoreSettings(dirs, sharding);
                
                foreach (var shardRestorePoints in pointsPerShard)
                {
                    Assert.True(ShardHelper.TryGetShardNumberFromDatabaseName(shardRestorePoints.Key, out var shardNumber));
                    Assert.True(settings.Shards.ContainsKey(shardNumber));

                    var points = shardRestorePoints.ToList();
                    Assert.Equal(3, points.Count);

                    // restore up to 2nd backup file (out of 3)
                    var lastFileToRestore = points[1].FileName;
                    settings.Shards[shardNumber].LastFileNameToRestore = lastFileToRestore;
                }

                var databaseName = $"restored_database-{Guid.NewGuid()}";
                using (Sharding.Backup.ReadOnly(backupPath))
                using (Backup.RestoreDatabase(store, new RestoreBackupConfiguration
                {
                    DatabaseName = databaseName,
                    ShardRestoreSettings = settings
                }, timeout: TimeSpan.FromSeconds(60)))
                {
                    var dbRec = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(databaseName));
                    Assert.Equal(3, dbRec.Sharding.Shards.Count);

                    using (var session = store.OpenSession(databaseName))
                    {
                        for (int i = 0; i < 10; i++)
                        {
                            var doc = session.Load<User>($"users/{i}");
                            Assert.NotNull(doc);

                            var counter = session.CountersFor(doc).Get("downloads");
                            Assert.Equal(i, counter);

                            var order = session.Load<Order>($"orders/{i}");
                            Assert.Null(order);
                        }
                    }
                }
            }
        }

        [AmazonS3RetryFact]
        public async Task CanRestoreShardedDatabase_UsingRestorePoint_FromS3Backup()
        {
            var s3Settings = GetS3Settings();

            try
            {
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

                    var config = Backup.CreateBackupConfiguration(s3Settings: s3Settings);
                    var backupTaskId = await Sharding.Backup.UpdateConfigurationAndRunBackupAsync(cluster.Nodes, store, config, isFullBackup: false);

                    Assert.True(WaitHandle.WaitAll(waitHandles, TimeSpan.FromMinutes(1)));

                    // add counters
                    waitHandles = await Sharding.Backup.WaitForBackupsToComplete(cluster.Nodes, store.Database);
                    using (var session = store.OpenAsyncSession())
                    {
                        for (int i = 0; i < 10; i++)
                        {
                            session.CountersFor($"users/{i}").Increment("downloads", i);
                        }

                        await session.SaveChangesAsync();
                    }

                    await Sharding.Backup.RunBackupAsync(store, backupTaskId, isFullBackup: false);
                    Assert.True(WaitHandle.WaitAll(waitHandles, TimeSpan.FromMinutes(1)));

                    // add more docs
                    waitHandles = await Sharding.Backup.WaitForBackupsToComplete(cluster.Nodes, store.Database);
                    using (var session = store.OpenSession())
                    {
                        for (int i = 0; i < 10; i++)
                        {
                            session.Store(new Order(), $"orders/{i}");
                        }

                        session.SaveChanges();
                    }

                    await Sharding.Backup.RunBackupAsync(store, backupTaskId, isFullBackup: false);
                    Assert.True(WaitHandle.WaitAll(waitHandles, TimeSpan.FromMinutes(1)));

                    var sharding = await Sharding.GetShardingConfigurationAsync(store);
                    ShardedRestoreSettings settings;
                    using (var s3Client = new RavenAwsS3Client(s3Settings, DefaultBackupConfiguration))
                    {
                        var prefix = $"{s3Settings.RemoteFolderName}/";
                        var cloudObjects = await s3Client.ListObjectsAsync(prefix, "/", listFolders: true);

                        Assert.Equal(3, cloudObjects.FileInfoDetails.Count);

                        settings = Sharding.Backup.GenerateShardRestoreSettings(cloudObjects.FileInfoDetails.Select(fileInfo => fileInfo.FullPath).ToList(), sharding);
                    }

                    var client = store.GetRequestExecutor().HttpClient;
                    var data = new StringContent(JsonConvert.SerializeObject(s3Settings), Encoding.UTF8, "application/json");
                    var response = await client.SendAsync(new HttpRequestMessage(HttpMethod.Post, $"{store.Urls.First()}/admin/restore/points?type=S3")
                    {
                        Content = data
                    }.WithConventions(store.Conventions));
                    string result = await response.Content.ReadAsStringAsync();

                    var restorePoints = JsonConvert.DeserializeObject<RestorePoints>(result);
                    Assert.Equal(9, restorePoints.List.Count);

                    var pointsPerShard = restorePoints.List.GroupBy(p => p.DatabaseName).ToList();
                    Assert.Equal(3, pointsPerShard.Count);

                    foreach (var shardRestorePoints in pointsPerShard)
                    {
                        Assert.True(ShardHelper.TryGetShardNumberFromDatabaseName(shardRestorePoints.Key, out var shardNumber));
                        Assert.True(settings.Shards.ContainsKey(shardNumber));

                        var points = shardRestorePoints.ToList();
                        Assert.Equal(3, points.Count);

                        // restore up to 2nd backup file (out of 3)
                        var lastFileToRestore = points[1].FileName;
                        settings.Shards[shardNumber].LastFileNameToRestore = lastFileToRestore;
                    }

                    var databaseName = $"restored_database-{Guid.NewGuid()}";
                    using (Backup.RestoreDatabaseFromCloud(store, new RestoreFromS3Configuration
                    {
                        DatabaseName = databaseName,
                        ShardRestoreSettings = settings,
                        Settings = s3Settings
                    }, timeout: TimeSpan.FromSeconds(60)))
                    {
                        var dbRec = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(databaseName));
                        Assert.Equal(3, dbRec.Sharding.Shards.Count);

                        using (var session = store.OpenSession(databaseName))
                        {
                            for (int i = 0; i < 10; i++)
                            {
                                var doc = session.Load<User>($"users/{i}");
                                Assert.NotNull(doc);

                                var counter = session.CountersFor(doc).Get("downloads");
                                Assert.Equal(i, counter);

                                var order = session.Load<Order>($"orders/{i}");
                                Assert.Null(order);
                            }
                        }
                    }
                }

            }
            finally
            {
                await DeleteObjects(s3Settings);
            }
        }

        [AzureRetryFact]
        public async Task CanRestoreShardedDatabase_UsingRestorePoint_FromAzureBackup()
        {
            var azureSettings = GetAzureSettings();

            try
            {
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

                    var config = Backup.CreateBackupConfiguration(azureSettings: azureSettings);
                    var backupTaskId = await Sharding.Backup.UpdateConfigurationAndRunBackupAsync(cluster.Nodes, store, config, isFullBackup: false);

                    Assert.True(WaitHandle.WaitAll(waitHandles, TimeSpan.FromMinutes(1)));

                    // add counters
                    waitHandles = await Sharding.Backup.WaitForBackupsToComplete(cluster.Nodes, store.Database);
                    using (var session = store.OpenAsyncSession())
                    {
                        for (int i = 0; i < 10; i++)
                        {
                            session.CountersFor($"users/{i}").Increment("downloads", i);
                        }

                        await session.SaveChangesAsync();
                    }

                    await Sharding.Backup.RunBackupAsync(store, backupTaskId, isFullBackup: false);
                    Assert.True(WaitHandle.WaitAll(waitHandles, TimeSpan.FromMinutes(1)));

                    // add more docs
                    waitHandles = await Sharding.Backup.WaitForBackupsToComplete(cluster.Nodes, store.Database);
                    using (var session = store.OpenSession())
                    {
                        for (int i = 0; i < 10; i++)
                        {
                            session.Store(new Order(), $"orders/{i}");
                        }

                        session.SaveChanges();
                    }

                    await Sharding.Backup.RunBackupAsync(store, backupTaskId, isFullBackup: false);
                    Assert.True(WaitHandle.WaitAll(waitHandles, TimeSpan.FromMinutes(1)));

                    var sharding = await Sharding.GetShardingConfigurationAsync(store);
                    ShardedRestoreSettings settings;
                    using (var azureClient = RavenAzureClient.Create(azureSettings, DefaultBackupConfiguration))
                    {
                        var prefix = $"{azureSettings.RemoteFolderName}/";
                        var blobs = await azureClient.ListBlobsAsync(prefix, delimiter: "/", listFolders: true);
                        var folderNames = blobs.List.Select(item => item.Name).ToList();
                        Assert.Equal(3, folderNames.Count);

                        settings = Sharding.Backup.GenerateShardRestoreSettings(folderNames, sharding);
                    }

                    var client = store.GetRequestExecutor().HttpClient;
                    var data = new StringContent(JsonConvert.SerializeObject(azureSettings), Encoding.UTF8, "application/json");
                    var response = await client.SendAsync(new HttpRequestMessage(HttpMethod.Post, $"{store.Urls.First()}/admin/restore/points?type=Azure")
                    {
                        Content = data
                    }.WithConventions(store.Conventions));
                    string result = await response.Content.ReadAsStringAsync();

                    var restorePoints = JsonConvert.DeserializeObject<RestorePoints>(result);
                    Assert.Equal(9, restorePoints.List.Count);

                    var pointsPerShard = restorePoints.List.GroupBy(p => p.DatabaseName).ToList();
                    Assert.Equal(3, pointsPerShard.Count);

                    foreach (var shardRestorePoints in pointsPerShard)
                    {
                        Assert.True(ShardHelper.TryGetShardNumberFromDatabaseName(shardRestorePoints.Key, out var shardNumber));
                        Assert.True(settings.Shards.ContainsKey(shardNumber));

                        var points = shardRestorePoints.ToList();
                        Assert.Equal(3, points.Count);

                        // restore up to 2nd backup file (out of 3)
                        var lastFileToRestore = points[1].FileName;
                        settings.Shards[shardNumber].LastFileNameToRestore = lastFileToRestore;
                    }

                    var databaseName = $"restored_database-{Guid.NewGuid()}";
                    using (Backup.RestoreDatabaseFromCloud(store, new RestoreFromAzureConfiguration
                    {
                        DatabaseName = databaseName,
                        ShardRestoreSettings = settings,
                        Settings = azureSettings
                    }, timeout: TimeSpan.FromSeconds(60)))
                    {
                        var dbRec = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(databaseName));
                        Assert.Equal(3, dbRec.Sharding.Shards.Count);

                        using (var session = store.OpenSession(databaseName))
                        {
                            for (int i = 0; i < 10; i++)
                            {
                                var doc = session.Load<User>($"users/{i}");
                                Assert.NotNull(doc);

                                var counter = session.CountersFor(doc).Get("downloads");
                                Assert.Equal(i, counter);

                                var order = session.Load<Order>($"orders/{i}");
                                Assert.Null(order);
                            }
                        }
                    }
                }

            }
            finally
            {
                await DeleteObjects(azureSettings);
            }
        }

        [GoogleCloudRetryFact]
        public async Task CanRestoreShardedDatabase_UsingRestorePoint_FromGoogleCloudBackup()
        {
            var googleCloudSettings = GetGoogleCloudSettings();

            try
            {
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

                    var config = Backup.CreateBackupConfiguration(googleCloudSettings: googleCloudSettings);
                    var backupTaskId = await Sharding.Backup.UpdateConfigurationAndRunBackupAsync(cluster.Nodes, store, config, isFullBackup: false);

                    Assert.True(WaitHandle.WaitAll(waitHandles, TimeSpan.FromMinutes(1)));

                    // add counters
                    waitHandles = await Sharding.Backup.WaitForBackupsToComplete(cluster.Nodes, store.Database);
                    using (var session = store.OpenAsyncSession())
                    {
                        for (int i = 0; i < 10; i++)
                        {
                            session.CountersFor($"users/{i}").Increment("downloads", i);
                        }

                        await session.SaveChangesAsync();
                    }

                    await Sharding.Backup.RunBackupAsync(store, backupTaskId, isFullBackup: false);
                    Assert.True(WaitHandle.WaitAll(waitHandles, TimeSpan.FromMinutes(1)));

                    // add more docs
                    waitHandles = await Sharding.Backup.WaitForBackupsToComplete(cluster.Nodes, store.Database);
                    using (var session = store.OpenSession())
                    {
                        for (int i = 0; i < 10; i++)
                        {
                            session.Store(new Order(), $"orders/{i}");
                        }

                        session.SaveChanges();
                    }

                    await Sharding.Backup.RunBackupAsync(store, backupTaskId, isFullBackup: false);
                    Assert.True(WaitHandle.WaitAll(waitHandles, TimeSpan.FromMinutes(1)));

                    var sharding = await Sharding.GetShardingConfigurationAsync(store);
                    var dirs = new HashSet<string>();
                    ShardedRestoreSettings settings;

                    using (var googleCloudClient = new RavenGoogleCloudClient(googleCloudSettings, DefaultBackupConfiguration))
                    {
                        var objects = await googleCloudClient.ListObjectsAsync(googleCloudSettings.RemoteFolderName);
                        Assert.Equal(9, objects.Count);

                        foreach (var obj in objects)
                        {
                            var fileName = obj.Name;
                            var dir = GetDirectoryName(fileName);
                            dirs.Add(dir);
                        }

                        Assert.Equal(3, dirs.Count);
                        settings = Sharding.Backup.GenerateShardRestoreSettings(dirs, sharding);
                    }

                    var client = store.GetRequestExecutor().HttpClient;
                    var data = new StringContent(JsonConvert.SerializeObject(googleCloudSettings), Encoding.UTF8, "application/json");
                    var response = await client.SendAsync(new HttpRequestMessage(HttpMethod.Post, $"{store.Urls.First()}/admin/restore/points?type=GoogleCloud")
                    {
                        Content = data
                    }.WithConventions(store.Conventions));

                    string result = await response.Content.ReadAsStringAsync();

                    var restorePoints = JsonConvert.DeserializeObject<RestorePoints>(result);
                    Assert.Equal(9, restorePoints.List.Count);

                    var pointsPerShard = restorePoints.List.GroupBy(p => p.DatabaseName).ToList();
                    Assert.Equal(3, pointsPerShard.Count);

                    foreach (var shardRestorePoints in pointsPerShard)
                    {
                        Assert.True(ShardHelper.TryGetShardNumberFromDatabaseName(shardRestorePoints.Key, out var shardNumber));
                        Assert.True(settings.Shards.ContainsKey(shardNumber));

                        var points = shardRestorePoints.ToList();
                        Assert.Equal(3, points.Count);

                        // restore up to 2nd backup file (out of 3)
                        var lastFileToRestore = points[1].FileName;
                        settings.Shards[shardNumber].LastFileNameToRestore = lastFileToRestore;
                    }

                    var databaseName = $"restored_database-{Guid.NewGuid()}";
                    using (Backup.RestoreDatabaseFromCloud(store, new RestoreFromGoogleCloudConfiguration()
                    {
                        DatabaseName = databaseName,
                        ShardRestoreSettings = settings,
                        Settings = googleCloudSettings
                    }, timeout: TimeSpan.FromSeconds(60)))
                    {
                        var dbRec = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(databaseName));
                        Assert.Equal(3, dbRec.Sharding.Shards.Count);

                        using (var session = store.OpenSession(databaseName))
                        {
                            for (int i = 0; i < 10; i++)
                            {
                                var doc = session.Load<User>($"users/{i}");
                                Assert.NotNull(doc);

                                var counter = session.CountersFor(doc).Get("downloads");
                                Assert.Equal(i, counter);

                                var order = session.Load<Order>($"orders/{i}");
                                Assert.Null(order);
                            }
                        }
                    }
                }

            }
            finally
            {
                await DeleteObjects(googleCloudSettings);
            }
        }
        
        [AmazonS3RetryFact]
        public async Task CanRestoreShardedDatabase_FromServerWideBackup()
        {
            var s3Settings = GetS3Settings();

            try
            {
                DoNotReuseServer();

                using (var store = Sharding.GetDocumentStore())
                {
                    await Sharding.Backup.InsertData(store);

                    // use backup configuration script for S3 settings
                    var scriptPath = GenerateConfigurationScriptForS3(s3Settings, out var command);
                    var serverWideConfig = new ServerWideBackupConfiguration
                    {
                        FullBackupFrequency = "0 0 1 1 *",
                        Disabled = false,
                        S3Settings = new S3Settings
                        {
                            GetBackupConfigurationScript = new GetBackupConfigurationScript
                            {
                                Exec = command,
                                Arguments = scriptPath
                            }
                        }
                    };

                    // define server wide backup
                    await store.Maintenance.Server.SendAsync(new PutServerWideBackupConfigurationOperation(serverWideConfig));

                    // wait for backup to complete
                    var backupsDone = await Sharding.Backup.WaitForBackupToComplete(store);

                    var databaseRecord = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                    Assert.Equal(1, databaseRecord.PeriodicBackups.Count);

                    var taskId = databaseRecord.PeriodicBackups[0].TaskId;
                    await Sharding.Backup.RunBackupAsync(store, taskId, isFullBackup: true);

                    Assert.True(WaitHandle.WaitAll(backupsDone, TimeSpan.FromMinutes(1)));

                    var sharding = await Sharding.GetShardingConfigurationAsync(store);

                    ShardedRestoreSettings settings;
                    using (var s3Client = new RavenAwsS3Client(s3Settings, ShardedRestoreBackupTests.DefaultBackupConfiguration))
                    {
                        var prefix = $"{s3Settings.RemoteFolderName}/";
                        var cloudObjects = await s3Client.ListObjectsAsync(prefix, "/", listFolders: true);

                        // should have one root folder for all shards 
                        Assert.Equal(1, cloudObjects.FileInfoDetails.Count);

                        var rootFolderName = cloudObjects.FileInfoDetails[0].FullPath;

                        Assert.EndsWith($"{store.Database}/", rootFolderName);
                        Assert.DoesNotContain('$', rootFolderName);

                        var shardsBackupFolders = await s3Client.ListObjectsAsync(rootFolderName, "/", listFolders: true);

                        // one backup folder per shard
                        Assert.Equal(3, shardsBackupFolders.FileInfoDetails.Count);

                        var backupPaths = shardsBackupFolders.FileInfoDetails.Select(x => x.FullPath).ToList();

                        Assert.Contains(ShardHelper.ToShardName(store.Database, 0), backupPaths[0]);
                        Assert.Contains(ShardHelper.ToShardName(store.Database, 1), backupPaths[1]);
                        Assert.Contains(ShardHelper.ToShardName(store.Database, 2), backupPaths[2]);

                        settings = Sharding.Backup.GenerateShardRestoreSettings(backupPaths, sharding);
                    }

                    var restoredDatabaseName = $"restored_database-{Guid.NewGuid()}";
                    using (Backup.RestoreDatabaseFromCloud(store, new RestoreFromS3Configuration
                    {
                        DatabaseName = restoredDatabaseName,
                        ShardRestoreSettings = settings,
                        Settings = s3Settings
                    }, timeout: TimeSpan.FromSeconds(60)))
                    {
                        var dbRec = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(restoredDatabaseName));
                        Assert.Equal(3, dbRec.Sharding.Shards.Count);

                        await Sharding.Backup.CheckData(store, RavenDatabaseMode.Sharded, expectedRevisionsCount: 16, database: restoredDatabaseName);

                    }
                }
            }
            finally
            {
                await DeleteObjects(s3Settings);
            }

        }

        private static string GetDirectoryName(string path)
        {
            var index = path.LastIndexOf('/');
            if (index <= 0)
                return string.Empty;

            return path[..(index + 1)];
        }

        private S3Settings GetS3Settings([CallerMemberName] string caller = null)
        {
            var s3Settings = AmazonS3RetryFactAttribute.S3Settings;
            if (s3Settings == null)
                return null;

            var remoteFolderName = _restoreFromS3TestsPrefix;
            if (string.IsNullOrEmpty(caller) == false)
                remoteFolderName = $"{remoteFolderName}/{caller}";

            if (string.IsNullOrEmpty(s3Settings.RemoteFolderName) == false)
                remoteFolderName = $"{s3Settings.RemoteFolderName}/{remoteFolderName}";

            return new S3Settings(s3Settings)
            {
                RemoteFolderName = remoteFolderName
            };
        }

        private AzureSettings GetAzureSettings([CallerMemberName] string caller = null)
        {
            var settings = AzureRetryFactAttribute.AzureSettings;
            if (settings == null)
                return null;

            var remoteFolderName = _restoreFromAzureTestsPrefix;
            if (string.IsNullOrEmpty(caller) == false)
                remoteFolderName = $"{remoteFolderName}/{caller}";

            if (string.IsNullOrEmpty(settings.RemoteFolderName) == false)
                remoteFolderName = $"{settings.RemoteFolderName}/{remoteFolderName}";

            return new AzureSettings(settings)
            {
                RemoteFolderName = remoteFolderName
            };
        }

        private GoogleCloudSettings GetGoogleCloudSettings([CallerMemberName] string caller = null)
        {
            var googleCloudSettings = GoogleCloudRetryFactAttribute.GoogleCloudSettings;
            if (googleCloudSettings == null)
                return null;

            var remoteFolderName = _restoreFromGoogleCloudTestsPrefix;
            if (string.IsNullOrEmpty(caller) == false)
                remoteFolderName = $"{remoteFolderName}/{caller}";

            if (string.IsNullOrEmpty(googleCloudSettings.RemoteFolderName) == false)
                remoteFolderName = $"{googleCloudSettings.RemoteFolderName}/{remoteFolderName}";

            return new GoogleCloudSettings(googleCloudSettings)
            {
                RemoteFolderName = remoteFolderName
            };
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

        private static string GenerateConfigurationScriptForS3(S3Settings settings, out string command)
        {
            var scriptPath = Path.Combine(Path.GetTempPath(), Path.ChangeExtension(Guid.NewGuid().ToString(), ".ps1"));
            var s3SettingsString = JsonConvert.SerializeObject(settings);

            string script;
            if (PlatformDetails.RunningOnPosix)
            {
                command = "bash";
                script = $"#!/bin/bash\r\necho '{s3SettingsString}'";
                File.WriteAllText(scriptPath, script);
                Process.Start("chmod", $"700 {scriptPath}");
            }
            else
            {
                command = "powershell";
                script = $"echo '{s3SettingsString}'";
                File.WriteAllText(scriptPath, script);
            }

            return scriptPath;
        }

    }
}
