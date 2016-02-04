using System.Threading.Tasks;
using Raven.Json.Linq;
using System.Collections.Specialized;
using System.IO;
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.FileSystem.Bugs
{
    public class Queries : RavenFilesTestWithLogs
    {
        [Fact]
        public void CanQueryMultipleFiles()
        {
            var client = NewAsyncClient();
            var ms = new MemoryStream();
            var streamWriter = new StreamWriter(ms);
            var expected = new string('a', 1024);
            streamWriter.Write(expected);
            streamWriter.Flush();
            ms.Position = 0;

            client.UploadAsync("abc.txt", ms, new RavenJObject()).Wait();

            ms.Position = 0;
            client.UploadAsync("CorelVBAManual.PDF", ms, new RavenJObject
            {
                {"Filename", "CorelVBAManual.PDF"}
            }).Wait();

            ms.Position = 0;
            client.UploadAsync("TortoiseSVN-1.7.0.22068-x64-svn-1.7.0.msi", ms, new RavenJObject
            {
                {"Filename", "TortoiseSVN-1.7.0.22068-x64-svn-1.7.0.msi"}
            }).Wait();


            var fileInfos = client.SearchAsync("Filename:corelVBAManual.PDF").Result;

            Assert.Equal(1, fileInfos.Files.Count);
            Assert.Equal("CorelVBAManual.PDF", fileInfos.Files[0].Name);
        }

        [Fact]
        public async Task WillGetOneItemWhenSavingDocumentTwice()
        {
            var client = NewAsyncClient();
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
                await client.UploadAsync("CorelVBAManual.PDF", ms, new RavenJObject
                                                            {
                                                                {"Filename", "CorelVBAManual.PDF"}
                                                            });
            }

            ms.Position = 0;
            await client.UploadAsync("TortoiseSVN-1.7.0.22068-x64-svn-1.7.0.msi", ms, new RavenJObject
                                                                            {
                                                                                {"Filename", "TortoiseSVN-1.7.0.22068-x64-svn-1.7.0.msi"}
                                                                            });


            var fileInfos = await client.SearchAsync("Filename:corelVBAManual.PDF");

            Assert.Equal(1, fileInfos.Files.Count);
            Assert.Equal("CorelVBAManual.PDF", fileInfos.Files[0].Name);
        }

        [Fact]
        public async Task ShouldEncodeValues()
        {

            var client = NewAsyncClient(); 
            var ms = new MemoryStream();
            var streamWriter = new StreamWriter(ms);
            var expected = new string('a', 1024);
            streamWriter.Write(expected);
            streamWriter.Flush();
            ms.Position = 0;

            const string filename = "10 jQuery Transition Effects/Moving Elements with Style - DevSnippets.txt";
            await client.UploadAsync(filename, ms, new RavenJObject { {"Item", "10"} });

            var fileInfos = await client.SearchAsync("Item:10*");

            Assert.Equal(1, fileInfos.Files.Count);
            Assert.Equal("/" + filename, fileInfos.Files[0].FullPath);
        }
    }
}
