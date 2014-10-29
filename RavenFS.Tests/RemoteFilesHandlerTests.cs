using System.Text.RegularExpressions;
using Xunit;

namespace RavenFS.Tests
{
	public class RemoteFilesHandlerTests : RavenFsWebApiTest
	{
		[Fact]
		public void CanGetFilesList_Empty()
		{
			var str = WebClient.DownloadString(GetFsUrl("/files"));
			Assert.Equal("[]", str);
		}

		[Fact]
		public void CanPutFile()
		{
			var data = new string('a', 1024*128);
			WebClient.UploadString(GetFsUrl("/files/abc.txt"), "PUT", data);
			var downloadString = WebClient.DownloadString(GetFsUrl("/files/abc.txt"));
			Assert.Equal(data, downloadString);
		}

		[Fact]
		public void CanGetFilesList()
		{
			WebClient.UploadString(GetFsUrl("/files/abc.txt"), "PUT", "abc");
			var str = WebClient.DownloadString(GetFsUrl("/files"));
			Assert.True(Regex.IsMatch(str, "[{\"Name\":\"abc.txt\",\"TotalSize\":3,\"UploadedSize\":3,\"HumaneTotalSize\":\"3 Bytes\",\"HumaneUploadedSize\":\"3 Bytes\","
                + "\"Metadata\":{\"Last\\-Modified\":\"(.+?)\"}}]"));
		}

		[Fact]
		public void CanSetFileMetadata_Then_GetItFromFilesList()
		{
			WebClient.Headers["Test"] = "Value";
			WebClient.UploadString(GetFsUrl("/files/abc.txt"), "PUT", "abc");
			var str = WebClient.DownloadString(GetFsUrl("/files"));
		    Assert.True(Regex.IsMatch(str,
		                  "[{\"Name\":\"abc.txt\",\"TotalSize\":3,\"UploadedSize\":3,\"HumaneTotalSize\":\"3 Bytes\",\"HumaneUploadedSize\":\"3 Bytes\"," 
                          + "\"Metadata\":{\"Test\":\"Value\",\"Last\\-Modified\":\"(.+?)\"}}]"));
		}

		[Fact]
		public void CanSetFileMetadata_Then_GetItFromFile()
		{
			WebClient.Headers["Test"] = "Value";
			WebClient.UploadString(GetFsUrl("/files/abc.txt"), "PUT", "abc");
			var str = WebClient.DownloadString(GetFsUrl("/files/abc.txt"));
			Assert.Equal("abc", str);
			Assert.Equal("Value", WebClient.ResponseHeaders["Test"]);
		}
	}
}