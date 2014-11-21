using System.IO;
using System.Threading.Tasks;
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.FileSystem.Bugs
{
	public class UploadDownload : RavenFilesTestWithLogs
	{
		[Fact]
		public async Task ShouldWork()
		{
			var fs = NewAsyncClient();
			var ms = new MemoryStream();
			var streamWriter = new StreamWriter(ms);
			var expected = new string('a', 1024);
			streamWriter.Write(expected);
			streamWriter.Flush();
			ms.Position = 0;
			fs.UploadAsync("abc.txt", ms).Wait();

            var ms2 = await fs.DownloadAsync("abc.txt");
			var actual = new StreamReader(ms2).ReadToEnd();
			Assert.Equal(expected, actual);
		}
	}
}