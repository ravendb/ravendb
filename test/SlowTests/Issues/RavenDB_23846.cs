using System;
using System.IO.Compression;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Amazon.S3;
using Amazon.S3.Model;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Config;
using Raven.Server.Documents.PeriodicBackup.Aws;
using Raven.Server.ServerWide;
using SlowTests.Core.Utils.Entities;
using SlowTests.Server.Documents.ETL.Olap;
using SlowTests.Server.Documents.PeriodicBackup.Restore;
using Sparrow.Backups;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;
using BackupConfiguration = Raven.Server.Config.Categories.BackupConfiguration;
using S3StorageClass = Raven.Client.Documents.Operations.Backups.S3StorageClass;

namespace SlowTests.Issues;

public class RavenDB_23846 : RestoreFromS3
{
    private static readonly BackupConfiguration DefaultBackupConfiguration;

    static RavenDB_23846()
    {
        var configuration = RavenConfiguration.CreateForTesting("foo", ResourceType.Database);
        configuration.Initialize();

        DefaultBackupConfiguration = configuration.Backup;
    }

    public RavenDB_23846(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.BackupExportImport)]
    public async Task CanBackupAndRestoreWithDefault()
    {
        var s3Settings = GetS3Settings();
        try
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Golan" }, "users/1");
                    await session.SaveChangesAsync();
                }

                var config = Backup.CreateBackupConfiguration(
                    backupType: BackupType.Snapshot,
                    s3Settings: s3Settings);

                config.SnapshotSettings = new SnapshotSettings
                {
                    CompressionAlgorithm = SnapshotBackupCompressionAlgorithm.Zstd, CompressionLevel = CompressionLevel.Fastest
                };
                await Backup.UpdateConfigAndRunBackupAsync(Server, config, store);
                await Backup.WaitForBackupToComplete(store);

                var s3Client = new AmazonS3Client(
                    s3Settings.AwsAccessKey,
                    s3Settings.AwsSecretKey,
                    Amazon.RegionEndpoint.GetBySystemName(s3Settings.AwsRegionName));
                var list = await s3Client.ListObjectsV2Async(new ListObjectsV2Request { BucketName = s3Settings.BucketName, Prefix = s3Settings.RemoteFolderName });

                Assert.NotEmpty(list.S3Objects);

                var snapshotKey = list.S3Objects[0].Key;
                var head = await s3Client.GetObjectMetadataAsync(s3Settings.BucketName, snapshotKey);

                Assert.Null(head.StorageClass);

                var restoreSettings = new RestoreFromS3Configuration { Settings = s3Settings, DatabaseName = $"{store.Database}_restored" };
                var restoreOp = await store.Maintenance.Server.SendAsync(new RestoreBackupOperation(restoreSettings));

                await restoreOp.WaitForCompletionAsync(TimeSpan.FromMinutes(10));

                using var restored = GetDocumentStore(new Options { CreateDatabase = false, ModifyDatabaseName = _ => restoreSettings.DatabaseName });
                using var restoredSession = restored.OpenSession();
                var user = restoredSession.Load<User>("users/1");
                Assert.Equal("Golan", user.Name);
            }
        }
        finally
        {
            await S3Tests.DeleteObjects(s3Settings, prefix: $"{s3Settings.RemoteFolderName}", delimiter: string.Empty);
        }
    }

    [RavenFact(RavenTestCategory.BackupExportImport | RavenTestCategory.Sharding)]
    public async Task CanBackupAndRestoreWithIntelligentTieringSharding()
    {
        var s3Settings = GetS3Settings();
        s3Settings.StorageClass = S3StorageClass.IntelligentTiering;
        try
        {
            using (var store = Sharding.GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Golan" }, "users/1");
                    await session.SaveChangesAsync();
                }

                var waitHandles = await Sharding.Backup.WaitForBackupToComplete(store);
                var config = Backup.CreateBackupConfiguration(
                    backupType: BackupType.Backup,
                    s3Settings: s3Settings);
                await Sharding.Backup.UpdateConfigurationAndRunBackupAsync(Server, store, config);

                Assert.True(WaitHandle.WaitAll(waitHandles, TimeSpan.FromMinutes(1)));

                var shardingCfg = await Sharding.GetShardingConfigurationAsync(store);

                using var ravens3 = new RavenAwsS3Client(s3Settings, DefaultBackupConfiguration);
                var prefix = s3Settings.RemoteFolderName + "/";
                var cloudObjects = await ravens3.ListObjectsAsync(prefix, "/", listFolders: true);

                Assert.Equal(3, cloudObjects.FileInfoDetails.Count);

                using var s3Client = new AmazonS3Client(
                    s3Settings.AwsAccessKey,
                    s3Settings.AwsSecretKey,
                    Amazon.RegionEndpoint.GetBySystemName(s3Settings.AwsRegionName));
                var list = await s3Client.ListObjectsV2Async(new ListObjectsV2Request { BucketName = s3Settings.BucketName, Prefix = s3Settings.RemoteFolderName });

                Assert.Equal(3, list.S3Objects.Count);

                foreach (var obj in list.S3Objects)
                {
                    var head = await s3Client.GetObjectMetadataAsync(s3Settings.BucketName, obj.Key);
                    Assert.Equal("INTELLIGENT_TIERING", head.StorageClass?.Value);
                }

                var settings = Sharding.Backup.GenerateShardRestoreSettings(cloudObjects.FileInfoDetails.Select(f => f.FullPath).ToList(), shardingCfg);
                var databaseName = $"{store.Database}_restored";
                var restoreSettings = new RestoreFromS3Configuration { Settings = s3Settings, DatabaseName = databaseName, ShardRestoreSettings = settings };
                using (Backup.RestoreDatabaseFromCloud(store,
                           restoreSettings,
                           timeout: TimeSpan.FromSeconds(60)))
                {
                    using var restored = Sharding.GetDocumentStore(new Options { CreateDatabase = false, ModifyDatabaseName = _ => restoreSettings.DatabaseName });
                    using var restoredSession = restored.OpenSession();
                    var user = restoredSession.Load<User>("users/1");
                    Assert.Equal("Golan", user.Name);
                }
            }
        }
        finally
        {
            await S3Tests.DeleteObjects(s3Settings);
        }
    }

    [RavenFact(RavenTestCategory.BackupExportImport)]
    public async Task CanBackupAndRestoreWithStandard()
    {
        var s3Settings = GetS3Settings();
        s3Settings.StorageClass = S3StorageClass.Standard;
        try
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Golan" }, "users/1");
                    await session.SaveChangesAsync();
                }

                var config = Backup.CreateBackupConfiguration(
                    backupType: BackupType.Snapshot,
                    s3Settings: s3Settings);

                config.SnapshotSettings = new SnapshotSettings
                {
                    CompressionAlgorithm = SnapshotBackupCompressionAlgorithm.Zstd, CompressionLevel = CompressionLevel.Fastest
                };
                await Backup.UpdateConfigAndRunBackupAsync(Server, config, store);
                await Backup.WaitForBackupToComplete(store);

                var s3Client = new AmazonS3Client(
                    s3Settings.AwsAccessKey,
                    s3Settings.AwsSecretKey,
                    Amazon.RegionEndpoint.GetBySystemName(s3Settings.AwsRegionName));
                var list = await s3Client.ListObjectsV2Async(new ListObjectsV2Request { BucketName = s3Settings.BucketName, Prefix = s3Settings.RemoteFolderName });

                Assert.NotEmpty(list.S3Objects);

                var snapshotKey = list.S3Objects[0].Key;
                var head = await s3Client.GetObjectMetadataAsync(s3Settings.BucketName, snapshotKey);

                Assert.Null(head.StorageClass);

                var restoreSettings = new RestoreFromS3Configuration { Settings = s3Settings, DatabaseName = $"{store.Database}_restored" };
                var restoreOp = await store.Maintenance.Server.SendAsync(new RestoreBackupOperation(restoreSettings));
                await restoreOp.WaitForCompletionAsync(TimeSpan.FromMinutes(10));

                using var restored = GetDocumentStore(new Options { CreateDatabase = false, ModifyDatabaseName = _ => restoreSettings.DatabaseName });
                using var restoredSession = restored.OpenSession();
                var user = restoredSession.Load<User>("users/1");
                Assert.Equal("Golan", user.Name);
            }
        }
        finally
        {
            await S3Tests.DeleteObjects(s3Settings);
        }
    }

    [RavenFact(RavenTestCategory.BackupExportImport)]
    public async Task CanBackupAndRestoreWithStandardInfrequentAccess()
    {
        var s3Settings = GetS3Settings();
        s3Settings.StorageClass = S3StorageClass.StandardInfrequentAccess;
        try
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Golan" }, "users/1");
                    await session.SaveChangesAsync();
                }

                var config = Backup.CreateBackupConfiguration(
                    backupType: BackupType.Snapshot,
                    s3Settings: s3Settings);

                config.SnapshotSettings = new SnapshotSettings
                {
                    CompressionAlgorithm = SnapshotBackupCompressionAlgorithm.Zstd, CompressionLevel = CompressionLevel.Fastest
                };

                await Backup.UpdateConfigAndRunBackupAsync(Server, config, store);
                await Backup.WaitForBackupToComplete(store);

                var s3Client = new AmazonS3Client(
                    s3Settings.AwsAccessKey,
                    s3Settings.AwsSecretKey,
                    Amazon.RegionEndpoint.GetBySystemName(s3Settings.AwsRegionName));
                var list = await s3Client.ListObjectsV2Async(new ListObjectsV2Request { BucketName = s3Settings.BucketName, Prefix = s3Settings.RemoteFolderName });
                Assert.NotEmpty(list.S3Objects);

                var snapshotKey = list.S3Objects[0].Key;
                var head = await s3Client.GetObjectMetadataAsync(s3Settings.BucketName, snapshotKey);
                Assert.Equal("STANDARD_IA", head.StorageClass?.Value);

                var restoreSettings = new RestoreFromS3Configuration { Settings = s3Settings, DatabaseName = $"{store.Database}_restored" };
                var restoreOp = await store.Maintenance.Server.SendAsync(new RestoreBackupOperation(restoreSettings));

                await restoreOp.WaitForCompletionAsync(TimeSpan.FromMinutes(10));

                using var restored = GetDocumentStore(new Options { CreateDatabase = false, ModifyDatabaseName = _ => restoreSettings.DatabaseName });
                using var restoredSession = restored.OpenSession();
                var user = restoredSession.Load<User>("users/1");
                Assert.Equal("Golan", user.Name);
            }
        }
        finally
        {
            await S3Tests.DeleteObjects(s3Settings);
        }
    }

    [RavenFact(RavenTestCategory.BackupExportImport)]
    public async Task Can_backup_and_restore_with_intelligent_tiering()
    {
        var s3Settings = GetS3Settings();
        s3Settings.StorageClass = S3StorageClass.IntelligentTiering;
        try
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Golan" }, "users/1");
                    await session.SaveChangesAsync();
                }

                var config = Backup.CreateBackupConfiguration(
                    backupType: BackupType.Snapshot,
                    s3Settings: s3Settings);

                config.SnapshotSettings = new SnapshotSettings
                {
                    CompressionAlgorithm = SnapshotBackupCompressionAlgorithm.Zstd, CompressionLevel = CompressionLevel.Fastest
                };

                await Backup.UpdateConfigAndRunBackupAsync(Server, config, store);
                await Backup.WaitForBackupToComplete(store);

                var s3Client = new AmazonS3Client(
                    s3Settings.AwsAccessKey,
                    s3Settings.AwsSecretKey,
                    Amazon.RegionEndpoint.GetBySystemName(s3Settings.AwsRegionName));
                var list = await s3Client.ListObjectsV2Async(new ListObjectsV2Request { BucketName = s3Settings.BucketName, Prefix = s3Settings.RemoteFolderName });

                Assert.NotEmpty(list.S3Objects);

                var snapshotKey = list.S3Objects[0].Key;
                var head = await s3Client.GetObjectMetadataAsync(s3Settings.BucketName, snapshotKey);

                Assert.Equal("INTELLIGENT_TIERING", head.StorageClass?.Value);

                var restoreSettings = new RestoreFromS3Configuration { Settings = s3Settings, DatabaseName = $"{store.Database}_restored" };
                var restoreOp = await store.Maintenance.Server.SendAsync(new RestoreBackupOperation(restoreSettings));
                await restoreOp.WaitForCompletionAsync(TimeSpan.FromMinutes(10));

                using var restored = GetDocumentStore(new Options { CreateDatabase = false, ModifyDatabaseName = _ => restoreSettings.DatabaseName });
                using var restoredSession = restored.OpenSession();
                var user = restoredSession.Load<User>("users/1");
                Assert.Equal("Golan", user.Name);
            }
        }
        finally
        {
            await S3Tests.DeleteObjects(s3Settings);
        }
    }

    [RavenFact(RavenTestCategory.BackupExportImport)]
    public async Task Can_backup_and_restore_with_glacier_instant_retrieval()
    {
        var s3Settings = GetS3Settings();
        s3Settings.StorageClass = S3StorageClass.GlacierInstantRetrieval;
        try
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Golan" }, "users/1");
                    await session.SaveChangesAsync();
                }

                var config = Backup.CreateBackupConfiguration(
                    backupType: BackupType.Snapshot,
                    s3Settings: s3Settings);

                config.SnapshotSettings = new SnapshotSettings
                {
                    CompressionAlgorithm = SnapshotBackupCompressionAlgorithm.Zstd, CompressionLevel = CompressionLevel.Fastest
                };

                await Backup.UpdateConfigAndRunBackupAsync(Server, config, store);
                await Backup.WaitForBackupToComplete(store);

                var s3Client = new AmazonS3Client(
                    s3Settings.AwsAccessKey,
                    s3Settings.AwsSecretKey,
                    Amazon.RegionEndpoint.GetBySystemName(s3Settings.AwsRegionName));
                var list = await s3Client.ListObjectsV2Async(new ListObjectsV2Request { BucketName = s3Settings.BucketName, Prefix = s3Settings.RemoteFolderName });

                Assert.NotEmpty(list.S3Objects);

                var snapshotKey = list.S3Objects[0].Key;
                var head = await s3Client.GetObjectMetadataAsync(s3Settings.BucketName, snapshotKey);

                Assert.Equal("GLACIER_IR", head.StorageClass?.Value);

                var restoreSettings = new RestoreFromS3Configuration { Settings = s3Settings, DatabaseName = $"{store.Database}_restored" };
                var restoreOp = await store.Maintenance.Server.SendAsync(new RestoreBackupOperation(restoreSettings));
                await restoreOp.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                using var restored = GetDocumentStore(new Options { CreateDatabase = false, ModifyDatabaseName = _ => restoreSettings.DatabaseName });
                using var restoredSession = restored.OpenSession();
                var user = restoredSession.Load<User>("users/1");
                Assert.Equal("Golan", user.Name);
            }
        }
        finally
        {
            await S3Tests.DeleteObjects(s3Settings, prefix: $"{s3Settings.RemoteFolderName}", delimiter: string.Empty);
        }
    }

    [RavenFact(RavenTestCategory.BackupExportImport)]
    public async Task Can_backup_and_restore_with_one_zone_infrequent_access()
    {
        var s3Settings = GetS3Settings();
        s3Settings.StorageClass = S3StorageClass.OneZoneInfrequentAccess;
        try
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Golan" }, "users/1");
                    await session.SaveChangesAsync();
                }

                var config = Backup.CreateBackupConfiguration(
                    backupType: BackupType.Snapshot,
                    s3Settings: s3Settings);

                config.SnapshotSettings = new SnapshotSettings
                {
                    CompressionAlgorithm = SnapshotBackupCompressionAlgorithm.Zstd, CompressionLevel = CompressionLevel.Fastest
                };

                await Backup.UpdateConfigAndRunBackupAsync(Server, config, store);
                await Backup.WaitForBackupToComplete(store);

                var s3Client = new AmazonS3Client(
                    s3Settings.AwsAccessKey,
                    s3Settings.AwsSecretKey,
                    Amazon.RegionEndpoint.GetBySystemName(s3Settings.AwsRegionName));
                var list = await s3Client.ListObjectsV2Async(new ListObjectsV2Request { BucketName = s3Settings.BucketName, Prefix = s3Settings.RemoteFolderName });

                Assert.NotEmpty(list.S3Objects);
                var snapshotKey = list.S3Objects[0].Key;
                var head = await s3Client.GetObjectMetadataAsync(s3Settings.BucketName, snapshotKey);

                Assert.Equal("ONEZONE_IA", head.StorageClass?.Value);

                var restoreSettings = new RestoreFromS3Configuration { Settings = s3Settings, DatabaseName = $"{store.Database}_restored" };
                var restoreOp = await store.Maintenance.Server.SendAsync(new RestoreBackupOperation(restoreSettings));
                await restoreOp.WaitForCompletionAsync(TimeSpan.FromMinutes(10));

                using var restored = GetDocumentStore(new Options { CreateDatabase = false, ModifyDatabaseName = _ => restoreSettings.DatabaseName });
                using var restoredSession = restored.OpenSession();
                var user = restoredSession.Load<User>("users/1");
                Assert.Equal("Golan", user.Name);
            }
        }
        finally
        {
            await S3Tests.DeleteObjects(s3Settings);
        }
    }

    [RavenFact(RavenTestCategory.BackupExportImport)]
    public async Task Can_backup_and_restore_reduced_redundancy()
    {
        var s3Settings = GetS3Settings();
        s3Settings.StorageClass = S3StorageClass.ReducedRedundancy;
        try
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Golan" }, "users/1");
                    await session.SaveChangesAsync();
                }

                var config = Backup.CreateBackupConfiguration(
                    backupType: BackupType.Snapshot,
                    s3Settings: s3Settings);

                config.SnapshotSettings = new SnapshotSettings
                {
                    CompressionAlgorithm = SnapshotBackupCompressionAlgorithm.Zstd, CompressionLevel = CompressionLevel.Fastest
                };

                await Backup.UpdateConfigAndRunBackupAsync(Server, config, store);
                await Backup.WaitForBackupToComplete(store);

                var s3Client = new AmazonS3Client(
                    s3Settings.AwsAccessKey,
                    s3Settings.AwsSecretKey,
                    Amazon.RegionEndpoint.GetBySystemName(s3Settings.AwsRegionName));
                var list = await s3Client.ListObjectsV2Async(new ListObjectsV2Request { BucketName = s3Settings.BucketName, Prefix = s3Settings.RemoteFolderName });

                Assert.NotEmpty(list.S3Objects);

                var snapshotKey = list.S3Objects[0].Key;
                var head = await s3Client.GetObjectMetadataAsync(s3Settings.BucketName, snapshotKey);

                Assert.Equal("REDUCED_REDUNDANCY", head.StorageClass?.Value);

                var restoreSettings = new RestoreFromS3Configuration { Settings = s3Settings, DatabaseName = $"{store.Database}_restored" };
                var restoreOp = await store.Maintenance.Server.SendAsync(new RestoreBackupOperation(restoreSettings));
                await restoreOp.WaitForCompletionAsync(TimeSpan.FromMinutes(10));

                using var restored = GetDocumentStore(new Options { CreateDatabase = false, ModifyDatabaseName = _ => restoreSettings.DatabaseName });
                using var restoredSession = restored.OpenSession();
                var user = restoredSession.Load<User>("users/1");
                Assert.Equal("Golan", user.Name);
            }
        }
        finally
        {
            await S3Tests.DeleteObjects(s3Settings);
        }
    }
}
