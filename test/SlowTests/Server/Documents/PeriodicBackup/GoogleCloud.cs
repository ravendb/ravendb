using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.Documents.PeriodicBackup.GoogleCloud;
using Sparrow;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.PeriodicBackup
{
    public class GoogleStorage : CloudBackupTestBase
    {
        public GoogleStorage(ITestOutputHelper output) : base(output)
        {
        }

        [GoogleCloudRetryFact]
        public void list_buckets()
        {
            using (var cts = new CancellationTokenSource(TimeSpan.FromMinutes(5)))
            using (var client = new RavenGoogleCloudClient(GoogleCloudRetryFactAttribute.GoogleCloudSettings, DefaultConfiguration, cancellationToken: cts.Token))
            {
                var buckets = client.ListBuckets();
                foreach (var b in buckets)
                {
                    Assert.NotNull(b.Name);
                }
            }
        }

        [GoogleCloudRetryFact]
        public async Task uploading_objects()
        {
            var fileName = Guid.NewGuid().ToString();
            using (var cts = new CancellationTokenSource(TimeSpan.FromMinutes(5)))
            using (var client = new RavenGoogleCloudClient(GoogleCloudRetryFactAttribute.GoogleCloudSettings, DefaultConfiguration, cancellationToken: cts.Token))
            {
                try
                {
                    await client.UploadObjectAsync(
                        fileName,
                        new MemoryStream(Encoding.UTF8.GetBytes("123"))
                    );
                }

                finally
                {
                    await client.DeleteObjectAsync(fileName);
                }
            }
        }

        [GoogleCloudRetryFact]
        public async Task download_objects()
        {
            var fileName = Guid.NewGuid().ToString();
            using (var cts = new CancellationTokenSource(TimeSpan.FromMinutes(5)))
            using (var client = new RavenGoogleCloudClient(GoogleCloudRetryFactAttribute.GoogleCloudSettings, DefaultConfiguration, cancellationToken: cts.Token))
            {
                try
                {
                    await client.UploadObjectAsync(
                        fileName,
                        new MemoryStream(Encoding.UTF8.GetBytes("123"))
                    );

                    var stream = client.DownloadObject(fileName);
                    using (var sr = new StreamReader(stream))
                    {
                        Assert.Equal("123", sr.ReadToEnd());
                    }
                }

                finally
                {
                    await client.DeleteObjectAsync(fileName);
                }
            }
        }

        [GoogleCloudRetryFact]
        public async Task delete_objects()
        {
            var fileName = Guid.NewGuid().ToString();
            using (var cts = new CancellationTokenSource(TimeSpan.FromMinutes(5)))
            using (var client = new RavenGoogleCloudClient(GoogleCloudRetryFactAttribute.GoogleCloudSettings, DefaultConfiguration, cancellationToken: cts.Token))
            {
                await client.UploadObjectAsync(
                    fileName,
                    new MemoryStream(Encoding.UTF8.GetBytes("123"))
                );

                var fileExists = await WaitForValueAsync(() => IsFileNameInCloudObjects(client, fileName), true);
                Assert.True(fileExists);

                await client.DeleteObjectAsync(fileName);
                
                fileExists = await WaitForValueAsync(() => IsFileNameInCloudObjects(client, fileName), false);
                Assert.False(fileExists);
            }
        }

        private async Task<bool> IsFileNameInCloudObjects(RavenGoogleCloudClient client, string fileName)
        {
            var cloudObjects = await client.ListObjectsAsync();
            var containsFileName = cloudObjects?.Any(x => x.Name == fileName) ?? false;
            return containsFileName;
        }

        [GoogleCloudRetryFact]
        public async Task upload_object_with_metadata()
        {
            var fileName = Guid.NewGuid().ToString();
            using (var cts = new CancellationTokenSource(TimeSpan.FromMinutes(5)))
            using (var client = new RavenGoogleCloudClient(GoogleCloudRetryFactAttribute.GoogleCloudSettings, DefaultConfiguration, cancellationToken: cts.Token))
            {
                try
                {
                    await client.UploadObjectAsync(
                        fileName,
                        new MemoryStream(Encoding.UTF8.GetBytes("456")),
                        new Dictionary<string, string>
                        {
                            { "key1", "value1" },
                            { "key2", "value2" }
                        });
                    var obj = await client.GetObjectAsync(fileName);
                    Assert.Equal("value1", obj.Metadata["key1"]);
                    Assert.Equal("value2", obj.Metadata["key2"]);

                    var size = await client.GetObjectSizeAsync(fileName);
                    Assert.True(size.GetValue(SizeUnit.Bytes) > 0);
                }

                finally
                {
                    await client.DeleteObjectAsync(fileName);
                }
            }
        }

        [GoogleCloudRetryFact]
        public async Task list_objects()
        {
            var file1 = "file1.txt";
            var file2 = "folder1/file2.txt";
            using (var cts = new CancellationTokenSource(TimeSpan.FromMinutes(5)))
            using (var client = new RavenGoogleCloudClient(GoogleCloudRetryFactAttribute.GoogleCloudSettings, DefaultConfiguration, cancellationToken: cts.Token))
            {
                try
                {
                    // Upload some files
                    var content = Encoding.UTF8.GetBytes("hello, world");
                    await client.UploadObjectAsync(file1, new MemoryStream(content));
                    await client.UploadObjectAsync(file2, new MemoryStream(content));

                    var objects = await client.ListObjectsAsync();
                    Assert.Contains(objects, o => o.Name == file1);
                    Assert.Contains(objects, o => o.Name == file2);
                }

                finally
                {
                    await client.DeleteObjectAsync(file1);
                    await client.DeleteObjectAsync(file2);
                }
            }
        }

        [GoogleCloudRetryFact]
        public async Task direct_upload_streams_unaligned_parts_into_a_single_object()
        {
            var fileName = $"{nameof(GoogleStorage)}-{Guid.NewGuid()}";
            using (var cts = new CancellationTokenSource(TimeSpan.FromMinutes(5)))
            using (var client = new RavenGoogleCloudClient(GoogleCloudRetryFactAttribute.GoogleCloudSettings, DefaultConfiguration, cancellationToken: cts.Token))
            {
                // the smallest chunk size, so every part spans several requests of the resumable upload
                client.DirectUploadChunkSize = new Size(256, SizeUnit.Kilobytes);

                try
                {
                    var uploader = client.GetUploader(fileName, new Dictionary<string, string> { { "Description", "Full backup" } });
                    uploader.Initialize();

                    var expected = new MemoryStream();
                    foreach (var size in new[] { 300_001, 1_000_000, 7, 2_165_000 })
                    {
                        var data = new byte[size];
                        Random.Shared.NextBytes(data);
                        expected.Write(data);

                        using (var part = new MemoryStream(data))
                            await uploader.UploadPartAsync(part);
                    }

                    await uploader.CompleteUploadAsync();

                    var obj = await client.GetObjectAsync(fileName);
                    Assert.Equal("Full backup", obj.Metadata["Description"]);
                    Assert.Equal((ulong)expected.Length, obj.Size);

                    var downloaded = new MemoryStream();
                    await using (var stream = client.DownloadObject(fileName))
                        await stream.CopyToAsync(downloaded, cts.Token);

                    Assert.Equal(expected.ToArray(), downloaded.ToArray());
                }
                finally
                {
                    await DeleteIfExistsAsync(client, fileName);
                }
            }
        }

        [GoogleCloudRetryFact]
        public async Task direct_upload_abort_does_not_create_an_object()
        {
            var fileName = $"{nameof(GoogleStorage)}-{Guid.NewGuid()}";
            using (var cts = new CancellationTokenSource(TimeSpan.FromMinutes(5)))
            using (var client = new RavenGoogleCloudClient(GoogleCloudRetryFactAttribute.GoogleCloudSettings, DefaultConfiguration, cancellationToken: cts.Token))
            {
                client.DirectUploadChunkSize = new Size(256, SizeUnit.Kilobytes);

                try
                {
                    var uploader = client.GetUploader(fileName, new Dictionary<string, string>());
                    uploader.Initialize();

                    var data = new byte[3 * 1024 * 1024 + 13];
                    Random.Shared.NextBytes(data);
                    using (var part = new MemoryStream(data))
                        await uploader.UploadPartAsync(part);

                    await uploader.AbortAsync();

                    Assert.Null(await client.GetObjectMetadataAsync(fileName));
                }
                finally
                {
                    await DeleteIfExistsAsync(client, fileName);
                }
            }
        }

        private static async Task DeleteIfExistsAsync(RavenGoogleCloudClient client, string fileName)
        {
            try
            {
                await client.DeleteObjectAsync(fileName);
            }
            catch
            {
                // the object might not exist
            }
        }
    }
}
