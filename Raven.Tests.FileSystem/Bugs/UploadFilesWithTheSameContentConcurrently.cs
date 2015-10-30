using System.Collections.Concurrent;
using System.IO;
using System.Threading.Tasks;
using Raven.Database.Extensions;
using Xunit;

namespace Raven.Tests.FileSystem.Bugs
{
    public class UploadFilesWithTheSameContentConcurrently : RavenFilesTestWithLogs
    {
        [Fact]
        public async Task ShouldWork()
        {
            var client = NewAsyncClient();
            var tasks = new ConcurrentBag<Task>();

            // upload 10 files with the same content but different names concurrently
            Assert.DoesNotThrow(
                () =>
                Parallel.For(0, 10, x => tasks.Add(client.UploadAsync("test" + x, new MemoryStream(new byte[] {1, 2, 3, 4, 5})))));

            Task.WaitAll(tasks.ToArray());

            var hash = new MemoryStream(new byte[] {1, 2, 3, 4, 5}).GetMD5Hash();

            for (var i = 0; i < 10; i++)
            {
                 var uploadedContent = await client.DownloadAsync("test" + i);
                Assert.Equal(hash, uploadedContent.GetMD5Hash());
            }
        }
    }
}
