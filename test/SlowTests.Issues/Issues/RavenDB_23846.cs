using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
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
using Sparrow;
using Sparrow.Backups;
using Tests.Infrastructure;
using Xunit;
using BackupConfiguration = Raven.Server.Config.Categories.BackupConfiguration;
using S3StorageClass = Raven.Client.Documents.Operations.Backups.S3StorageClass;

namespace SlowTests.Issues;

public class RavenDB_23846 : RestoreFromS3TestBase
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

    [RavenFact(RavenTestCategory.BackupExportImport | RavenTestCategory.Sharding, Requires = RavenServiceRequirement.Aws)]
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
            await DeleteObjects(s3Settings);
        }
    }

    [AmazonS3RetryTheory]
    [InlineData(11, false, UploadType.Chunked, false)]
    [InlineData(11, true, UploadType.Chunked, false)]
    public async Task PutObjectAsync(int sizeInMB, bool testBlobKeyAsFolder, UploadType uploadType, bool noAsciiDbName)
    {
        var settings = GetS3Settings();
        settings.StorageClass = S3StorageClass.IntelligentTiering;
        var blobs = GenerateBlobNames(settings, 1, out _);
        Assert.Equal(1, blobs.Count);
        var key = "";

        var progress = new Raven.Server.Documents.PeriodicBackup.Progress();
        using (var cts = new CancellationTokenSource(TimeSpan.FromMinutes(5)))
        using (var client = new RavenAwsS3Client(settings, DefaultConfiguration, progress, cts.Token))
        {
            client.MaxUploadPutObject = new Sparrow.Size(10, SizeUnit.Megabytes);
            client.MinOnePartUploadSizeLimit = new Sparrow.Size(7, SizeUnit.Megabytes);

            var property1 = "property1";
            var property2 = "property2";
            var value1 = Guid.NewGuid().ToString();
            var value2 = Guid.NewGuid().ToString();
            if (noAsciiDbName)
            {
                string dateStr = DateTime.Now.ToString("yyyy-MM-dd-HH-mm-ss-fff");
                key = $"{dateStr}.ravendb-żżżרייבן-A-backup/{dateStr}.ravendb-full-backup";
                property1 = "Description-żżרייבן";
                value1 = "ravendb-żżżרייבן-A-backup";
            }
            else
            {
                key = $"{blobs[0]}";
            }

            if (testBlobKeyAsFolder)
                key += "/";


            var sb = new StringBuilder();
            for (var i = 0; i < sizeInMB * 1024 * 1024; i++)
            {
                sb.Append("a");
            }

            long streamLength;
            using (var memoryStream = new MemoryStream(Encoding.UTF8.GetBytes(sb.ToString())))
            {
                streamLength = memoryStream.Length;
                await client.PutObjectAsync(key,
                    memoryStream,
                    new Dictionary<string, string> { { property1, value1 }, { property2, value2 } });
            }

            var @object = await client.GetObjectAsync(key);
            Assert.NotNull(@object);

            using (var reader = new StreamReader(@object.Data))
                Assert.Equal(sb.ToString(), await reader.ReadToEndAsync(cts.Token));

            var property1check = @object.Metadata.Keys.Single(x => x.Contains(Uri.EscapeDataString(property1).ToLower()));
            var property2check = @object.Metadata.Keys.Single(x => x.Contains(property2));

            Assert.Equal(Uri.EscapeDataString(value1), @object.Metadata[property1check]);
            Assert.Equal(value2, @object.Metadata[property2check]);

            Assert.Equal(UploadState.Done, progress.UploadProgress.UploadState);
            Assert.Equal(uploadType, progress.UploadProgress.UploadType);
            Assert.Equal(streamLength, progress.UploadProgress.TotalInBytes);
            Assert.Equal(streamLength, progress.UploadProgress.UploadedInBytes);
        }
    }


    [AmazonS3RetryTheory]
    [InlineData(null)]
    [InlineData(S3StorageClass.Standard)]
    [InlineData(S3StorageClass.StandardInfrequentAccess)]
    [InlineData(S3StorageClass.OneZoneInfrequentAccess)]
    [InlineData(S3StorageClass.IntelligentTiering)]
    [InlineData(S3StorageClass.GlacierInstantRetrieval)]
    [InlineData(S3StorageClass.ReducedRedundancy)]
    public async Task Can_backup_and_restore_with_various_storage_classes(S3StorageClass? storageClass)
    {
        var s3Settings = GetS3Settings();
        if (storageClass.HasValue)
            s3Settings.StorageClass = storageClass.Value;

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
                    CompressionAlgorithm = SnapshotBackupCompressionAlgorithm.Zstd,
                    CompressionLevel = CompressionLevel.Fastest
                };

                await Backup.UpdateConfigAndRunBackupAsync(Server, config, store);
                await Backup.WaitForBackupToComplete(store);

                using var s3 = new AmazonS3Client(
                    s3Settings.AwsAccessKey,
                    s3Settings.AwsSecretKey,
                    Amazon.RegionEndpoint.GetBySystemName(s3Settings.AwsRegionName));

                var list = await s3.ListObjectsV2Async(
                    new ListObjectsV2Request
                    {
                        BucketName = s3Settings.BucketName,
                        Prefix = s3Settings.RemoteFolderName
                    });
                Assert.NotEmpty(list.S3Objects);

                var objKey = list.S3Objects[0].Key;
                var metadata = await s3.GetObjectMetadataAsync(s3Settings.BucketName, objKey);

                var expected = GetExpectedStorageClassValue(storageClass);
                if (expected == null)
                    Assert.Null(metadata.StorageClass);
                else
                    Assert.Equal(expected, metadata.StorageClass?.Value);

                var restoreSettings = new RestoreFromS3Configuration
                {
                    Settings = s3Settings,
                    DatabaseName = $"{store.Database}_restored"
                };

                var op = await store.Maintenance.Server.SendAsync(new RestoreBackupOperation(restoreSettings));
                await op.WaitForCompletionAsync(TimeSpan.FromMinutes(10));

                using var restored = GetDocumentStore(
                    new Options { CreateDatabase = false, ModifyDatabaseName = _ => restoreSettings.DatabaseName });

                using var restoredSession = restored.OpenSession();
                var user = restoredSession.Load<User>("users/1");
                Assert.Equal("Golan", user.Name);
            }
        }
        finally
        {
            await DeleteObjects(s3Settings);   
        }
    }

    private string GetExpectedStorageClassValue(S3StorageClass? cls) => cls switch
    {
        null => null,
        S3StorageClass.Standard => null,
        S3StorageClass.StandardInfrequentAccess => "STANDARD_IA",
        S3StorageClass.OneZoneInfrequentAccess => "ONEZONE_IA",
        S3StorageClass.IntelligentTiering => "INTELLIGENT_TIERING",
        S3StorageClass.GlacierInstantRetrieval => "GLACIER_IR",
        S3StorageClass.ReducedRedundancy => "REDUCED_REDUNDANCY",
        _ => throw new ArgumentOutOfRangeException(nameof(cls), cls, $"Unknown S3 storage-class value: {cls}")
    };

}

