using System.IO;
using Xunit;

namespace RavenFS.Tests.Bugs
{
	public class UploadDownload : RavenFsTestBase
	{
		[Fact] 
		public async void ShouldWork()
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