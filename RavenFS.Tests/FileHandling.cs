using System.Net;
using Xunit;

namespace RavenFS.Tests
{
	public class FileHandling : WebApiTest
	{
		[Fact]
		public void CanOverwriteFiles()
		{
			WebClient.UploadString("/ravenfs/files/abc.txt", "PUT", "abcd");
			WebClient.UploadString("/ravenfs/files/abc.txt", "PUT", "efcg");

			var str = WebClient.DownloadString("/ravenfs/files/abc.txt");
			Assert.Equal("efcg", str);
		}

		[Fact]
		public void CanDeleteFiles()
		{
			WebClient.UploadString("/ravenfs/files/abc.txt", "PUT", "abcd");
			WebClient.UploadString("/ravenfs/files/abc.txt", "DELETE", "");

			var webException = Assert.Throws<WebException>(()=>WebClient.DownloadString("/files/abc.txt"));
			Assert.Equal(HttpStatusCode.NotFound, ((HttpWebResponse)webException.Response).StatusCode);
		}
	}
}