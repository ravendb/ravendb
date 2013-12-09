using System.Net;
using Xunit;

namespace RavenFS.Tests
{
	public class FileHandling : WebApiTest
	{
		[Fact]
		public void CanOverwriteFiles()
		{
			WebClient.UploadString("/files/abc.txt", "PUT", "abcd");
			WebClient.UploadString("/files/abc.txt", "PUT", "efcg");

			var str = WebClient.DownloadString("/files/abc.txt");
			Assert.Equal("efcg", str);
		}

		[Fact]
		public void CanDeleteFiles()
		{
			WebClient.UploadString("/files/abc.txt", "PUT", "abcd");
			WebClient.UploadString("/files/abc.txt", "DELETE", "");

			var webException = Assert.Throws<WebException>(()=>WebClient.DownloadString("/files/abc.txt"));
			Assert.Equal(HttpStatusCode.NotFound, ((HttpWebResponse)webException.Response).StatusCode);
		}
	}
}