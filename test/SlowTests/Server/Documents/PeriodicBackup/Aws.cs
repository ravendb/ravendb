using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Json;
using Raven.Client.Util;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.Documents.PeriodicBackup.Aws;
using SlowTests.Server.Documents.PeriodicBackup.Restore;
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

        private const string AwsAccessKey = "<aws_access_key>";
        private const string AwsSecretKey = "<aws_secret_key>";
        private const string AwsSessionToken = "<aws_session_token>";
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
                Assert.Equal($"AWS location is set to \"{region2}\", but the bucket named: \"'{bucketName}'\" is located in: {region1}", error2.Message);
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
                client.MaxUploadPutObjectSizeInBytes = 10 * 1024 * 1024; // 10MB
                client.MinOnePartUploadSizeLimitInBytes = 7 * 1024 * 1024; // 7MB

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

        [Theory(Skip = "Requires Amazon AWS Glacier Credentials")]
        [InlineData(EastRegion1)]
        [InlineData(WestRegion2)]
        public void upload_archive(string region)
        {
            var vaultName = $"testing-{Guid.NewGuid()}";

            using (var client = new RavenAwsGlacierClient(GetGlacierSettings(region, vaultName)))
            {
                client.PutVault();

                var archiveId = client.UploadArchive(
                    new MemoryStream(Encoding.UTF8.GetBytes("321")),
                    "sample description");

                Assert.NotNull(archiveId);
            }
        }

        [Theory(Skip = "Requires Amazon AWS Glacier Credentials")]
        [InlineData(EastRegion1)]
        [InlineData(WestRegion2)]
        public void upload_archive_with_remote_folder_name(string region)
        {
            var vaultName = $"testing-{Guid.NewGuid()}";

            var glacierSettings = GetGlacierSettings(region, vaultName);
            glacierSettings.RemoteFolderName = Guid.NewGuid().ToString();
            using (var client = new RavenAwsGlacierClient(glacierSettings))
            {
                client.PutVault();

                var archiveId = client.UploadArchive(
                    new MemoryStream(Encoding.UTF8.GetBytes("321")),
                    "sample description");

                Assert.NotNull(archiveId);
            }
        }

        [Theory(Skip = "Requires Amazon AWS Glacier Credentials")]
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

        [Theory(Skip = "Requires Amazon AWS Glacier Credentials")]
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
        public void upload_archive_multipart(string region, 
            int sizeInMB, int minOnePartSizeInMB, UploadType uploadType)
        {
            UploadArchive(region, sizeInMB, minOnePartSizeInMB, uploadType);
        }

        // ReSharper disable once InconsistentNaming
        private static void UploadArchive(string region, 
            int sizeInMB, int minOnePartSizeInMB, UploadType uploadType)
        {
            var vaultName = $"testing-{Guid.NewGuid()}";

            var progress = new Progress();
            var maxUploadArchiveSizeInBytes = ExpressionHelper.CreateFieldSetter<RavenAwsGlacierClient, int>("MaxUploadArchiveSizeInBytes");
            var minOnePartUploadSizeLimitInBytes = ExpressionHelper.CreateFieldSetter<RavenAwsGlacierClient, int>("MinOnePartUploadSizeLimitInBytes");

            using (var client = new RavenAwsGlacierClient(GetGlacierSettings(region, vaultName), progress))
            {
                maxUploadArchiveSizeInBytes(client, 10 * 1024 * 1024); // 9MB
                minOnePartUploadSizeLimitInBytes(client, minOnePartSizeInMB * 1024 * 1024);

                client.PutVault();

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

        [Fact]
        public void AuthorizationHeaderValueForAwsS3ShouldBeCalculatedCorrectly1()
        {
            var s3Settings = new S3Settings
            {
                AwsAccessKey = "AKIAIOSFODNN7EXAMPLE",
                AwsSecretKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
                AwsRegionName = "us-east-1",
                BucketName = "examplebucket"
            };

            using (var client = new RavenAwsS3Client(s3Settings))
            {
                var date = new DateTime(2013, 5, 24);

                var stream = new MemoryStream(Encoding.UTF8.GetBytes("Welcome to Amazon S3."));
                var payloadHash = RavenAwsHelper.CalculatePayloadHash(stream);

                Assert.Equal("44ce7dd67c959e0d3524ffac1771dfbba87d2b6b4b4e99e42034a8b803f8b072", payloadHash);

                var url = client.GetUrl() + "/test%24file.text";
                var headers = new Dictionary<string, string>
                {
                    {"x-amz-date", RavenAwsHelper.ConvertToString(date)},
                    {"x-amz-content-sha256", payloadHash},
                    {"x-amz-storage-class", "REDUCED_REDUNDANCY"},
                    {"Date", date.ToString("R")},
                    {"Host", "s3.amazonaws.com"}
                };

                var auth = client.CalculateAuthorizationHeaderValue(HttpMethods.Put, url, date, headers);

                Assert.Equal("AWS4-HMAC-SHA256", auth.Scheme);
                Assert.Equal("Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,SignedHeaders=date;host;x-amz-content-sha256;x-amz-date;x-amz-storage-class,Signature=d2dd2e48b10d2cb89c271a6464d0748686c158b5fde44e8d83936fd9b30b5c4c", auth.Parameter);
            }
        }

        [Fact]
        public void AuthorizationHeaderValueForAwsS3ShouldBeCalculatedCorrectly2()
        {
            var s3Settings = new S3Settings
            {
                AwsAccessKey = "AKIAIOSFODNN7EXAMPLE",
                AwsSecretKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
                AwsRegionName = "us-east-1",
                BucketName = "examplebucket"
            };

            using (var client = new RavenAwsS3Client(s3Settings))
            {
                var date = new DateTime(2013, 5, 24);
                var payloadHash = RavenAwsHelper.CalculatePayloadHash(null);

                Assert.Equal("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", payloadHash);

                var url = client.GetUrl() + "/test.txt";
                var headers = new Dictionary<string, string>
                {
                    {"x-amz-date", RavenAwsHelper.ConvertToString(date)},
                    {"x-amz-content-sha256", payloadHash},
                    {"Date", date.ToString("R")},
                    {"Host", "s3.amazonaws.com"},
                    {"Range", "bytes=0-9"}
                };

                var auth = client.CalculateAuthorizationHeaderValue(HttpMethods.Get, url, date, headers);

                Assert.Equal("AWS4-HMAC-SHA256", auth.Scheme);
                Assert.Equal("Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,SignedHeaders=host;range;x-amz-content-sha256;x-amz-date,Signature=819484c483cfb97d16522b1ac156f87e61677cc8f1f2545c799650ef178f4aa8", auth.Parameter);
            }
        }

        private static GlacierSettings GetGlacierSettings(string region, string vaultName)
        {
            return new GlacierSettings
            {
                AwsAccessKey = AwsAccessKey,
                AwsSecretKey = AwsSecretKey,
                AwsSessionToken = AwsSessionToken,
                AwsRegionName = region,
                VaultName = vaultName
            };
        }
    }
}
