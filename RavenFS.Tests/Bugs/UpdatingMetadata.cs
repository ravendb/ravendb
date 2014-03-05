using System.Collections.Specialized;
using System.IO;
using Xunit;

namespace RavenFS.Tests.Bugs
{
    public class UpdatingMetadata : RavenFsTestBase
	{
		[Fact]
		public void CanUpdateMetadata()
		{
			var client = NewClient(); 
			var ms = new MemoryStream();
			var streamWriter = new StreamWriter(ms);
			var expected = new string('a', 1024);
			streamWriter.Write(expected);
			streamWriter.Flush();
			ms.Position = 0;

			client.UploadAsync("abc.txt", new NameValueCollection
			{
				{"test", "1"}
			}, ms).Wait();

			client.UpdateMetadataAsync("abc.txt", new NameValueCollection
			{
				{"test", "2"}
			}).Wait();

			var metadataFor = client.GetMetadataForAsync("abc.txt");


			Assert.Equal("2", metadataFor.Result["test"]);
		}

		 
	}
}