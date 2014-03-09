using System.IO;
using Xunit;

namespace RavenFS.Tests.Bugs
{
	public class UploadDownload : RavenFsTestBase
	{
		[Fact] 
		public void ShouldWork()
		{
			var fs = NewClient();
			var ms = new MemoryStream();
			var streamWriter = new StreamWriter(ms);
			var expected = new string('a', 1024);
			streamWriter.Write(expected);
			streamWriter.Flush();
			ms.Position = 0;
			fs.UploadAsync("abc.txt", ms).Wait();

			var ms2 = new MemoryStream();
			fs.DownloadAsync("abc.txt", ms2).Wait();

			ms2.Position = 0;

			var actual = new StreamReader(ms2).ReadToEnd();
			Assert.Equal(expected, actual);
		}
	}
}