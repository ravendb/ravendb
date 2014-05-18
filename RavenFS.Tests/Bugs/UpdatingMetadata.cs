using Raven.Json.Linq;
using System.Collections.Specialized;
using System.IO;
using Xunit;

namespace RavenFS.Tests.Bugs
{
    public class UpdatingMetadata : RavenFsTestBase
	{
		[Fact]
		public async void CanUpdateMetadata()
		{
			var client = NewClient(); 
			var ms = new MemoryStream();
			var streamWriter = new StreamWriter(ms);
			var expected = new string('a', 1024);
			streamWriter.Write(expected);
			streamWriter.Flush();
			ms.Position = 0;

            await client.UploadAsync("abc.txt", new RavenJObject
			                                        {
				                                        {"test", "1"}
			                                        }, ms);

            await client.UpdateMetadataAsync("abc.txt", new RavenJObject
			                                                {
				                                                {"test", "2"}
			                                                });

			var metadataFor = await client.GetMetadataForAsync("abc.txt");


			Assert.Equal("2", metadataFor["test"]);
		}

		 
	}
}