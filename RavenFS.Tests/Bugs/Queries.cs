using Raven.Json.Linq;
using System.Collections.Specialized;
using System.IO;
using Xunit;

namespace RavenFS.Tests.Bugs
{
    public class Queries : RavenFsTestBase
	{
		[Fact]
		public void CanQueryMultipleFiles()
		{
			var client = NewClient();
			var ms = new MemoryStream();
			var streamWriter = new StreamWriter(ms);
			var expected = new string('a', 1024);
			streamWriter.Write(expected);
			streamWriter.Flush();
			ms.Position = 0;

            client.UploadAsync("abc.txt", new RavenJObject(), ms).Wait();

			ms.Position = 0;
            client.UploadAsync("CorelVBAManual.PDF", new RavenJObject
			{
				{"Filename", "CorelVBAManual.PDF"}
			}, ms).Wait();

			ms.Position = 0;
            client.UploadAsync("TortoiseSVN-1.7.0.22068-x64-svn-1.7.0.msi", new RavenJObject
			{
				{"Filename", "TortoiseSVN-1.7.0.22068-x64-svn-1.7.0.msi"}
			}, ms).Wait();


			var fileInfos = client.SearchAsync("Filename:corelVBAManual.PDF").Result;

			Assert.Equal(1, fileInfos.Files.Length);
			Assert.Equal("CorelVBAManual.PDF", fileInfos.Files[0].Name);
		}

		[Fact]
		public async void WillGetOneItemWhenSavingDocumentTwice()
		{
			var client = NewClient();
			var ms = new MemoryStream();
			var streamWriter = new StreamWriter(ms);
			var expected = new string('a', 1024);
			streamWriter.Write(expected);
			streamWriter.Flush();
			ms.Position = 0;

            await client.UploadAsync("abc.txt", ms);

			for (int i = 0; i < 3; i++)
			{
				ms.Position = 0;
                await client.UploadAsync("CorelVBAManual.PDF", new RavenJObject
				                                            {
					                                            {"Filename", "CorelVBAManual.PDF"}
				                                            }, ms);
			}

			ms.Position = 0;
            await client.UploadAsync("TortoiseSVN-1.7.0.22068-x64-svn-1.7.0.msi", new RavenJObject
			                                                                {
				                                                                {"Filename", "TortoiseSVN-1.7.0.22068-x64-svn-1.7.0.msi"}
			                                                                }, ms);


			var fileInfos = await client.SearchAsync("Filename:corelVBAManual.PDF");

			Assert.Equal(1, fileInfos.Files.Length);
			Assert.Equal("CorelVBAManual.PDF", fileInfos.Files[0].Name);
		}

		[Fact]
		public void ShouldEncodeValues()
		{

			var client = NewClient(); 
			var ms = new MemoryStream();
			var streamWriter = new StreamWriter(ms);
			var expected = new string('a', 1024);
			streamWriter.Write(expected);
			streamWriter.Flush();
			ms.Position = 0;

			const string filename = "10 jQuery Transition Effects/Moving Elements with Style - DevSnippets.txt";
            client.UploadAsync(filename, new RavenJObject
			{
				{"Item", "10"}
			}, ms).Wait();


			var fileInfos = client.SearchAsync("Item:10*").Result;

			Assert.Equal(1, fileInfos.Files.Length);
			Assert.Equal("/" + filename, fileInfos.Files[0].Name);
		}
	}
}