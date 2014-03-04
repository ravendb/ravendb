using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Raven.Database.Server.RavenFS.Extensions;
using Xunit;

namespace RavenFS.Tests.Bugs
{
    public class UploadFilesWithTheSameContentConcurrently : RavenFsTestBase
	{
		[Fact]
		public void ShouldWork()
		{
			var client = NewClient();
			var tasks = new List<Task>(10);

			// upload 10 files with the same content but different names concurrently
			Assert.DoesNotThrow(
				() =>
				Parallel.For(0, 10, x => tasks.Add(client.UploadAsync("test" + x, new MemoryStream(new byte[] {1, 2, 3, 4, 5})))));

			Task.WaitAll(tasks.ToArray());

			var hash = new MemoryStream(new byte[] {1, 2, 3, 4, 5}).GetMD5Hash();

			for (var i = 0; i < 10; i++)
			{
				var uploadedContent = new MemoryStream();
				client.DownloadAsync("test" + i, uploadedContent).Wait();

				uploadedContent.Position = 0;

				Assert.Equal(hash, uploadedContent.GetMD5Hash());
			}
		}
	}
}