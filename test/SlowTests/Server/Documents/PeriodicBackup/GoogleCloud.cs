using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FastTests;
using Raven.Server.Documents.PeriodicBackup.GoogleCloud;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.PeriodicBackup
{
    public class GoogleStorage : RavenTestBase
    {
        public GoogleStorage(ITestOutputHelper output) : base(output)
        {
        }

        [GoogleCloudFact]
        public void list_buckets()
        {
            using (var client = new RavenGoogleCloudClient(GoogleCloudFactAttribute.GoogleCloudSettings))
            {
                var buckets = client.ListBuckets();
                foreach (var b in buckets)
                {
                    Assert.NotNull(b.Name);
                }
            }
        }

        [GoogleCloudFact]
        public async Task uploading_objects()
        {
            var fileName = Guid.NewGuid().ToString();
            using (var client = new RavenGoogleCloudClient(GoogleCloudFactAttribute.GoogleCloudSettings))
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

        [GoogleCloudFact]
        public async Task download_objects()
        {
            var fileName = Guid.NewGuid().ToString();
            using (var client = new RavenGoogleCloudClient(GoogleCloudFactAttribute.GoogleCloudSettings))
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

        [GoogleCloudFact]
        public async Task delete_objects()
        {
            var fileName = Guid.NewGuid().ToString();
            using (var client = new RavenGoogleCloudClient(GoogleCloudFactAttribute.GoogleCloudSettings))
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

        [GoogleCloudFact]
        public async Task upload_object_with_metadata()
        {
            var fileName = Guid.NewGuid().ToString();
            using (var client = new RavenGoogleCloudClient(GoogleCloudFactAttribute.GoogleCloudSettings))
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
                }

                finally
                {
                    await client.DeleteObjectAsync(fileName);
                }
            }
        }

        [GoogleCloudFact]
        public async Task list_objects()
        {
            var file1 = "file1.txt";
            var file2 = "folder1/file2.txt";
            using (var client = new RavenGoogleCloudClient(GoogleCloudFactAttribute.GoogleCloudSettings))
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
    }
}
