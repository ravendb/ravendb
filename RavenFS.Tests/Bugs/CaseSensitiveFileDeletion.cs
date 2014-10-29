using System.IO;
using Xunit;

namespace RavenFS.Tests.Bugs
{
    public class CaseSensitiveFileDeletion : RavenFsTestBase
    {
        [Fact]
        public void FilesWithUpperCaseNamesAreDeletedProperly()
        {
            var client = NewAsyncClient();
            var ms = new MemoryStream();
            client.UploadAsync("Abc.txt", ms).Wait();

            client.DeleteAsync("Abc.txt").Wait();

            var result = client.SearchOnDirectoryAsync("/").Result;

            Assert.Equal(0, result.FileCount);
        }
    }
}
