using System.IO;
using Xunit;

namespace RavenFS.Tests.Bugs
{
    public class CaseSensitiveFileDeletion : RavenFsTestBase
    {
        [Fact]
        public void FilesWithUpperCaseNamesAreDeletedProperly()
        {
            var client = NewClient();
            var ms = new MemoryStream();
            client.UploadAsync("Abc.txt", ms).Wait();

            client.DeleteAsync("Abc.txt").Wait();

            var result = client.GetFilesAsync("/").Result;

            Assert.Equal(0, result.FileCount);
        }
    }
}
