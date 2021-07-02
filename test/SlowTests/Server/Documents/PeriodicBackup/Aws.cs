using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Json;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.Documents.PeriodicBackup.Aws;
using SlowTests.Server.Documents.PeriodicBackup.Restore;
using Sparrow;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.PeriodicBackup
{
    public class Aws : RestoreFromS3
    {
        public Aws(ITestOutputHelper output) : base(output)
        {
        }

        private const string EastRegion1 = "us-east-1";
        private const string WestRegion2 = "us-west-2";

        [AmazonS3Fact]
        public async Task put_object()
        {
            var settings = GetS3Settings();
            using (var client = new RavenAwsS3Client(settings))
            {
                var blobs = GenerateBlobNames(settings, 1, out _);
                Assert.Equal(1, blobs.Count);
                var key = blobs[0];

                var value1 = Guid.NewGuid().ToString();
                var value2 = Guid.NewGuid().ToString();
                client.PutObject(key, new MemoryStream(Encoding.UTF8.GetBytes("231")), new Dictionary<string, string>
                    {
                        {"property1", value1},
                        {"property2", value2}
                    });

                var @object = await client.GetObjectAsync(key);
                Assert.NotNull(@object);

                using (var reader = new StreamReader(@object.Data))
                    Assert.Equal("231", reader.ReadToEnd());

                var property1 = @object.Metadata.Keys.Single(x => x.Contains("property1"));
                var property2 = @object.Metadata.Keys.Single(x => x.Contains("property2"));

                Assert.Equal(value1, @object.Metadata[property1]);
                Assert.Equal(value2, @object.Metadata[property2]);
            }
        }

        [AmazonS3Fact]
        public void can_get_correct_error_s3()
        {
            var settings = GetS3Settings();
            string region1 = settings.AwsRegionName;
            string region2 = settings.AwsRegionName = WestRegion2;
            var bucketName = settings.BucketName;
            using (var clientRegion2 = new RavenAwsS3Client(settings))
            {
                var sb = new StringBuilder();
                for (var i = 0; i < 1 * 1024 * 1024; i++)
                {
                    sb.Append("a");
                }
                var blobs = GenerateBlobNames(settings, 1, out _);
                Assert.Equal(1, blobs.Count);
                var key = blobs[0];
                var error2 = Assert.Throws<InvalidOperationException>(() =>
                    {
                        using (var memoryStream = new MemoryStream(Encoding.UTF8.GetBytes(sb.ToString())))
                        {
                            clientRegion2.PutObject(key,
                                memoryStream,
                                new Dictionary<string, string>());
                        }
                    });
                Assert.Equal($"AWS location is set to '{region2}', but the bucket named: '{bucketName}' is located in: {region1}", error2.Message);
            }
        }

        [AmazonS3Theory]
        [InlineData(5, false, UploadType.Regular)]
        [InlineData(5, true, UploadType.Regular)]
        [InlineData(11, false, UploadType.Chunked)]
        [InlineData(11, true, UploadType.Chunked)]
        // ReSharper disable once InconsistentNaming
        public async Task put_object_multipart(int sizeInMB, bool testBlobKeyAsFolder, UploadType uploadType)
        {
            await PutObject(sizeInMB, testBlobKeyAsFolder, uploadType);
        }

        // ReSharper disable once InconsistentNaming
        private async Task PutObject(int sizeInMB, bool testBlobKeyAsFolder, UploadType uploadType)
        {
            var settings = GetS3Settings();
            var blobs = GenerateBlobNames(settings, 1, out _);
            Assert.Equal(1, blobs.Count);
            var key = $"{blobs[0]}";
            if (testBlobKeyAsFolder)
                key += "/";

            var progress = new Progress();
            using (var client = new RavenAwsS3Client(settings, progress))
            {
                client.MaxUploadPutObject = new Sparrow.Size(10, SizeUnit.Megabytes);
                client.MinOnePartUploadSizeLimit = new Sparrow.Size(7, SizeUnit.Megabytes);

                var value1 = Guid.NewGuid().ToString();
                var value2 = Guid.NewGuid().ToString();

                var sb = new StringBuilder();
                for (var i = 0; i < sizeInMB * 1024 * 1024; i++)
                {
                    sb.Append("a");
                }

                long streamLength;
                using (var memoryStream = new MemoryStream(Encoding.UTF8.GetBytes(sb.ToString())))
                {
                    streamLength = memoryStream.Length;
                    client.PutObject(key,
                        memoryStream,
                        new Dictionary<string, string>
                        {
                                {"property1", value1},
                                {"property2", value2}
                        });
                }

                var @object = await client.GetObjectAsync(key);
                Assert.NotNull(@object);

                using (var reader = new StreamReader(@object.Data))
                    Assert.Equal(sb.ToString(), reader.ReadToEnd());

                var property1 = @object.Metadata.Keys.Single(x => x.Contains("property1"));
                var property2 = @object.Metadata.Keys.Single(x => x.Contains("property2"));

                Assert.Equal(value1, @object.Metadata[property1]);
                Assert.Equal(value2, @object.Metadata[property2]);

                Assert.Equal(UploadState.Done, progress.UploadProgress.UploadState);
                Assert.Equal(uploadType, progress.UploadProgress.UploadType);
                Assert.Equal(streamLength, progress.UploadProgress.TotalInBytes);
                Assert.Equal(streamLength, progress.UploadProgress.UploadedInBytes);
            }
        }

        [AmazonGlacierTheory]
        [InlineData(EastRegion1)]
        [InlineData(WestRegion2)]
        public async Task upload_archive(string region)
        {
            var vaultName = $"testing-{Guid.NewGuid()}";

            using (var client = new RavenAwsGlacierClient(GetGlacierSettings(region, vaultName)))
            {
                await client.PutVaultAsync();

                var archiveId = client.UploadArchive(
                    new MemoryStream(Encoding.UTF8.GetBytes("321")),
                    "sample description");

                Assert.NotNull(archiveId);
            }
        }

        [AmazonGlacierTheory]
        [InlineData(EastRegion1)]
        [InlineData(WestRegion2)]
        public async Task upload_archive_with_remote_folder_name(string region)
        {
            var vaultName = $"testing-{Guid.NewGuid()}";

            var glacierSettings = GetGlacierSettings(region, vaultName);
            glacierSettings.RemoteFolderName = Guid.NewGuid().ToString();
            using (var client = new RavenAwsGlacierClient(glacierSettings))
            {
                await client.PutVaultAsync();

                var archiveId = client.UploadArchive(
                    new MemoryStream(Encoding.UTF8.GetBytes("321")),
                    "sample description");

                Assert.NotNull(archiveId);
            }
        }

        [AmazonGlacierTheory]
        [InlineData(EastRegion1, WestRegion2)]
        [InlineData(WestRegion2, EastRegion1)]
        public void can_get_correct_error_glacier(string region1, string region2)
        {
            var vaultName1 = $"testing-{Guid.NewGuid()}";
            var vaultName2 = $"testing-{Guid.NewGuid()}";

            using (var clientRegion1 = new RavenAwsGlacierClient(GetGlacierSettings(region1, vaultName1)))
            using (var clientRegion2 = new RavenAwsGlacierClient(GetGlacierSettings(region2, vaultName2)))
            {
                var e = Assert.Throws<VaultNotFoundException>(() =>
                {
                    clientRegion2.UploadArchive(
                         new MemoryStream(Encoding.UTF8.GetBytes("321")),
                         "sample description");
                });
                Assert.Equal(e.Message, $"Vault name '{vaultName2}' doesn't exist in {region2}!");

                e = Assert.Throws<VaultNotFoundException>(() =>
                {
                    clientRegion1.UploadArchive(
                        new MemoryStream(Encoding.UTF8.GetBytes("321")),
                        "sample description");
                });
                Assert.Equal(e.Message, $"Vault name '{vaultName1}' doesn't exist in {region1}!");
            }
        }

        [AmazonGlacierTheory]
        [InlineData(EastRegion1, 5, 2, UploadType.Regular)]
        [InlineData(EastRegion1, 5, 3, UploadType.Regular)]
        [InlineData(WestRegion2, 5, 2, UploadType.Regular)]
        [InlineData(WestRegion2, 5, 3, UploadType.Regular)]
        [InlineData(EastRegion1, 9, 2, UploadType.Regular)]
        [InlineData(EastRegion1, 9, 3, UploadType.Regular)]
        [InlineData(WestRegion2, 9, 2, UploadType.Regular)]
        [InlineData(WestRegion2, 9, 3, UploadType.Regular)]
        [InlineData(EastRegion1, 11, 2, UploadType.Chunked)]
        [InlineData(EastRegion1, 11, 3, UploadType.Chunked)]
        [InlineData(WestRegion2, 11, 2, UploadType.Chunked)]
        [InlineData(WestRegion2, 11, 3, UploadType.Chunked)]
        [InlineData(EastRegion1, 13, 2, UploadType.Chunked)]
        [InlineData(EastRegion1, 13, 3, UploadType.Chunked)]
        [InlineData(WestRegion2, 13, 2, UploadType.Chunked)]
        [InlineData(WestRegion2, 13, 3, UploadType.Chunked)]
        [InlineData(EastRegion1, 14, 2, UploadType.Chunked)]
        [InlineData(EastRegion1, 14, 3, UploadType.Chunked)]
        [InlineData(WestRegion2, 14, 2, UploadType.Chunked)]
        [InlineData(WestRegion2, 14, 3, UploadType.Chunked)]
        public Task upload_archive_multipart(string region,
            int sizeInMB, int minOnePartSizeInMB, UploadType uploadType)
        {
            return UploadArchiveAsync(region, sizeInMB, minOnePartSizeInMB, uploadType);
        }

        // ReSharper disable once InconsistentNaming
        private static async Task UploadArchiveAsync(string region,
            int sizeInMB, int minOnePartSizeInMB, UploadType uploadType)
        {
            var vaultName = $"testing-{Guid.NewGuid()}";

            var progress = new Progress();

            using (var client = new RavenAwsGlacierClient(GetGlacierSettings(region, vaultName), progress))
            {
                client.MaxUploadArchiveSize = new Size(9, SizeUnit.Megabytes);
                client.MinOnePartUploadSizeLimit = new Size(minOnePartSizeInMB, SizeUnit.Megabytes);

                await client.PutVaultAsync();

                var sb = new StringBuilder();
                for (var i = 0; i < sizeInMB * 1024 * 1024; i++)
                {
                    sb.Append("a");
                }

                long streamLength;
                using (var memoryStream = new MemoryStream(Encoding.UTF8.GetBytes(sb.ToString())))
                {
                    streamLength = memoryStream.Length;
                    var archiveId = client.UploadArchive(memoryStream,
                        $"testing-upload-archive-{Guid.NewGuid()}");

                    Assert.NotNull(archiveId);
                }

                Assert.Equal(UploadState.Done, progress.UploadProgress.UploadState);
                Assert.Equal(uploadType, progress.UploadProgress.UploadType);
                Assert.Equal(streamLength, progress.UploadProgress.TotalInBytes);
                Assert.Equal(streamLength, progress.UploadProgress.UploadedInBytes);
            }
        }

        private static GlacierSettings GetGlacierSettings(string region, string vaultName)
        {
            var glacierSettings = AmazonGlacierFactAttribute.GlacierSettings;
            if (glacierSettings == null)
                return null;

            var settings = new GlacierSettings
            {
                VaultName = vaultName,
                AwsAccessKey = glacierSettings.AwsAccessKey,
                AwsSecretKey = glacierSettings.AwsSecretKey,
                AwsRegionName = region
            };

            return settings;
        }
    }
}
