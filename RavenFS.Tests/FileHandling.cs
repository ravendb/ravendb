using System.Net;
using Xunit;

namespace RavenFS.Tests
{
    public class FileHandling : RavenFsWebApiTest
	{
		[Fact]
		public void CanOverwriteFiles()
		{
			WebClient.UploadString(GetFsUrl("/files/abc.txt"), "PUT", "abcd");
            WebClient.UploadString(GetFsUrl("/files/abc.txt"), "PUT", "efcg");

			var str = WebClient.DownloadString(GetFsUrl("/files/abc.txt"));
			Assert.Equal("efcg", str);
		}

		[Fact]
		public void CanDeleteFiles()
		{
			WebClient.UploadString(GetFsUrl("/files/abc.txt"), "PUT", "abcd");
			WebClient.UploadString(GetFsUrl("/files/abc.txt"), "DELETE", "");

			var webException = Assert.Throws<WebException>(()=>WebClient.DownloadString(GetFsUrl("/files/abc.txt")));
			Assert.Equal(HttpStatusCode.NotFound, ((HttpWebResponse)webException.Response).StatusCode);
		}
	}
}