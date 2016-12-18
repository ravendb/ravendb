using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Xunit;
using Raven.Abstractions.FileSystem;

namespace Raven.Tests.FileSystem
{
    public class Folders : RavenFilesTestWithLogs
    {
        [Fact]
        public void CanGetListOfFolders()
        {
            var client = NewAsyncClient();
            var ms = new MemoryStream();
            client.UploadAsync("test/abc.txt", ms).Wait();
            client.UploadAsync("test/ced.txt", ms).Wait();
            client.UploadAsync("why/abc.txt", ms).Wait();

            var strings = client.GetDirectoriesAsync().Result;
            Assert.Equal(new[]{"/test", "/why"}, strings);
        }

        [Fact]
        public async Task WillNotGetNestedFolders()
        {
            var client = NewAsyncClient();
            var ms = new MemoryStream();
            await client.UploadAsync("test/c.txt", ms);
            await client.UploadAsync("test/ab/c.txt", ms);
            await client.UploadAsync("test/ce/d.txt", ms);
            await client.UploadAsync("test/ce/a/d.txt", ms);
            await client.UploadAsync("why/abc.txt", ms);

            var strings = await client.GetDirectoriesAsync();
            Assert.Equal(new[] { "/test", "/why" }, strings);

            strings = await client.GetDirectoriesAsync("test");
            Assert.Equal(new[] { "/test/ab", "/test/ce" }, strings);
        }

        [Fact]
        public async Task WillNotGetFoldersWithSharedPrefix()
        {
            var client = NewAsyncClient();
            var ms = new MemoryStream();
            await client.UploadAsync("test/aa/bb/cc/file.txt", ms);
            await client.UploadAsync("test/aa/b/dd/file.txt", ms);
            await client.UploadAsync("aa/b/test/file.txt", ms);

            var strings = await client.GetDirectoriesAsync("test/aa");
            Assert.Equal(new [] { "/test/aa/b", "/test/aa/bb"}, strings);

            strings = await client.GetDirectoriesAsync("test/aa/b");
            Assert.Equal(new[] { "/test/aa/b/dd" }, strings);

            strings = await client.GetDirectoriesAsync("test/aa/bb");
            Assert.Equal(new[] { "/test/aa/bb/cc" }, strings);

        }

        [Fact]
        public async Task WillWorkWithTrailingSlash()
        {
            var client = NewAsyncClient();
            var ms = new MemoryStream();
            await client.UploadAsync("test/ab/c.txt", ms);
            await client.UploadAsync("test/ce/d.txt", ms);
            await client.UploadAsync("test/ce/a/d.txt", ms);

            var strings = await client.GetDirectoriesAsync("test/");
            Assert.Equal(new[] { "/test/ab", "/test/ce" }, strings);
        }

        [Fact]
        public async Task WillUploadFileInNestedFolders()
        {
            var client = NewAsyncClient();
            var ms = new MemoryStream();
            var streamWriter = new StreamWriter(ms);
            var expected = new string('a', 1024);
            streamWriter.Write(expected);
            streamWriter.Flush();
            
            ms.Position = 0;
            await client.UploadAsync("test/something.txt", ms);
            ms.Position = 0;
            await client.UploadAsync("test/ab/c.txt", ms);

            new MemoryStream();
            var downloadStream = await client.DownloadAsync("test/ab/c.txt");

            Assert.Equal(expected, StreamToString(downloadStream));
        }

        [Fact]
        public async Task WillUploadFileInNestedFolders_InverseOrder()
        {
            var client = NewAsyncClient();
            var ms = new MemoryStream();
            var streamWriter = new StreamWriter(ms);
            var expected = new string('a', 1024);
            streamWriter.Write(expected);
            streamWriter.Flush();

            ms.Position = 0;
            await client.UploadAsync("test/ab/c.txt", ms);
            ms.Position = 0;
            await client.UploadAsync("test/something.txt", ms);

            var downloadStream = await client.DownloadAsync("test/ab/c.txt");
            Assert.Equal(expected, StreamToString(downloadStream));
        }

        [Fact]
        public void WillNotGetOtherFolders()
        {
            var client = NewAsyncClient();
            var ms = new MemoryStream();
            client.UploadAsync("test/ab/c.txt", ms).Wait();
            client.UploadAsync("test/ce/d.txt", ms).Wait();
            client.UploadAsync("test/ab/a/c.txt", ms).Wait();

            var strings = client.GetDirectoriesAsync("/test").Result;
            Assert.Equal(new[] {"/test/ab", "/test/ce" }, strings);
    
        }


        [Fact]
        public async Task CanRename()
        {
            var client = NewAsyncClient();
            var ms = new MemoryStream();
            await client.UploadAsync("test/abc.txt", ms);

            await client.RenameAsync("test/abc.txt", "test2/abc.txt");

            await client.DownloadAsync("test2/abc.txt");// would throw if missing
        }



        [Fact]
        public async Task AfterRename_OldFolderIsGoneAndWeHaveNewOne()
        {
            var client = NewAsyncClient();
            var ms = new MemoryStream();
            await client.UploadAsync("test/abc.txt", ms);

            Assert.Contains("/test", await client.GetDirectoriesAsync());

            await client.RenameAsync("test/abc.txt", "test2/abc.txt");

            await client.DownloadAsync("test2/abc.txt");// would throw if missing

            Assert.DoesNotContain("/test", await client.GetDirectoriesAsync());

            Assert.Contains("/test2", await client.GetDirectoriesAsync());
        }

        [Fact]
        public async Task AfterCopy_WeHaveNewFolder()
        {
            var client = NewAsyncClient();
            var ms = new MemoryStream();
            await client.UploadAsync("test/abc.txt", ms);

            Assert.Contains("/test", await client.GetDirectoriesAsync());

            await client.CopyAsync("test/abc.txt", "test2/abc.txt");

            await client.DownloadAsync("test2/abc.txt");// would throw if missing

            Assert.Contains("/test", await client.GetDirectoriesAsync());

            Assert.Contains("/test2", await client.GetDirectoriesAsync());
        }

        [Fact]
        public async Task CanGetListOfFilesInFolder()
        {
            var client = NewAsyncClient();
            var ms = new MemoryStream();
            await client.UploadAsync("test/abc.txt", ms);
            await client.UploadAsync("test/ced.txt", ms);
            await client.UploadAsync("why/abc.txt", ms);

            var results = await client.SearchOnDirectoryAsync("/test").ConfigureAwait(false);
            var strings = results.Files.Select(x => x.FullPath).ToArray();
            Assert.Equal(new[] { "/test/abc.txt", "/test/ced.txt" }, strings);
        }


        [Fact]
        public async Task CanGetListOfFilesInFolder_Sorted_Size()
        {
            var client = NewAsyncClient();
            await client.UploadAsync("test/abc.txt", new MemoryStream(new byte[4]));
            await client.UploadAsync("test/ced.txt", new MemoryStream(new byte[8]));

            var search = await client.SearchOnDirectoryAsync("/test", FilesSortOptions.Size | FilesSortOptions.Desc);
            Assert.Equal(new[] { "/test/ced.txt", "/test/abc.txt" }, search.Files.Select(x => x.FullPath).ToArray());
            Assert.Equal(new[] { "ced.txt", "abc.txt" }, search.Files.Select(x => x.Name).ToArray());
        }

        [Fact]
        public async Task CanGetListOfFilesInFolder_Sorted_Name()
        {
            var client = NewAsyncClient();
            await client.UploadAsync("test/abc.txt", new MemoryStream(new byte[4]));
            await client.UploadAsync("test/ced.txt", new MemoryStream(new byte[8]));

            var search = await client.SearchOnDirectoryAsync("/test", FilesSortOptions.Name | FilesSortOptions.Desc);
            Assert.Equal(new[] { "/test/ced.txt", "/test/abc.txt" }, search.Files.Select(x => x.FullPath).ToArray());
            Assert.Equal(new[] { "ced.txt", "abc.txt" }, search.Files.Select(x => x.Name).ToArray());
        }


        [Fact]
        public void CanGetListOfFilesInFolderInRoot()
        {
            var client = NewAsyncClient();
            var ms = new MemoryStream();
            client.UploadAsync("test/abc.txt", ms).Wait();
            client.UploadAsync("test/ced.txt", ms).Wait();
            client.UploadAsync("why/abc.txt", ms).Wait();

            var strings = client.SearchOnDirectoryAsync("/").Result.Files.Select(x => x.Name).ToArray();
            Assert.Equal(new string[] { }, strings);
        }


        [Fact]
        public async Task CanGetListOfFilesInFolder2()
        {
            var client = NewAsyncClient();
            var ms = new MemoryStream();
            await client.UploadAsync("test/abc.txt", ms);
            await client.UploadAsync("test/ced.txt", ms);
            await client.UploadAsync("why/abc.txt", ms);

            var search = await client.SearchOnDirectoryAsync("/test");
            Assert.Equal(new string[] { "/test/abc.txt", "/test/ced.txt" }, search.Files.Select(x => x.FullPath).ToArray());
            Assert.Equal(new string[] { "abc.txt", "ced.txt" }, search.Files.Select(x => x.Name).ToArray());
        }

        [Fact]
        public void CanSearchForFilesByPattern()
        {
            var client = NewAsyncClient();
            var ms = new MemoryStream();

            client.UploadAsync("abc.txt", ms).Wait();
            client.UploadAsync("def.txt", ms).Wait();
            client.UploadAsync("dhi.txt", ms).Wait();

            var fileNames =
                client.SearchOnDirectoryAsync("/", fileNameSearchPattern: "d*").Result.Files.Select(x => x.Name).ToArray();
            Assert.Equal(new string[] { "def.txt", "dhi.txt"}, fileNames);
        }

        [Fact]
        public void CanSearchForFilesByPatternBeginningWithMultiCharacterWildcard()
        {
            var client = NewAsyncClient();
            var ms = new MemoryStream();

            client.UploadAsync("abc.txt", ms).Wait();
            client.UploadAsync("def.txt", ms).Wait();
            client.UploadAsync("ghi.png", ms).Wait();
            client.UploadAsync("jkl.png", ms).Wait();

            var fileNames =
                client.SearchOnDirectoryAsync("/", fileNameSearchPattern: "*.png").Result.Files.Select(x => x.Name).ToArray();
            Assert.Equal(new string[] { "ghi.png", "jkl.png" }, fileNames);
        }

        [Fact]
        public void CanSearchForFilesByPatternBeginningWithSingleCharacterWildcard()
        {
            var client = NewAsyncClient();
            var ms = new MemoryStream();

            client.UploadAsync("abc.txt", ms).Wait();
            client.UploadAsync("def.txt", ms).Wait();
            client.UploadAsync("ghi.png", ms).Wait();
            client.UploadAsync("jkl.png", ms).Wait();

            var fileNames = client.SearchOnDirectoryAsync("/", fileNameSearchPattern: "?bc?txt").Result.Files.Select(x => x.Name).ToArray();
            Assert.Equal(new string[] { "abc.txt" }, fileNames);
        }

        [Fact]
        public void CanSearchForFilesByNameWithinAFolder()
        {
            var client = NewAsyncClient();
            var ms = new MemoryStream();

            client.UploadAsync("test/abc.txt", ms).Wait();
            client.UploadAsync("test/def.txt", ms).Wait();

            var fileNames = client.SearchOnDirectoryAsync("/test", fileNameSearchPattern: "a*").Result.Files.Select(x => x.FullPath).ToArray();
            Assert.Equal(new string[] { "/test/abc.txt" }, fileNames);
        }

        [Fact]
        public void CanSearchPatternContainingNoWildcardsDoesStartsWithSearchByDefault()
        {
            var client = NewAsyncClient();
            var ms = new MemoryStream();

            client.UploadAsync("abc.txt", ms).Wait();
            client.UploadAsync("def.txt", ms).Wait();

            var fileNames =
                client.SearchOnDirectoryAsync("/", fileNameSearchPattern: "a").Result.Files.Select(x => x.Name).ToArray();
            Assert.Equal(new string[] { "abc.txt" }, fileNames);
        }

        [Fact]
        public async Task CanPage()
        {
            var client = NewAsyncClient();
            var ms = new MemoryStream();
            await client.UploadAsync("test/abc.txt", ms);
            await client.UploadAsync("test/ced.txt", ms);
            await client.UploadAsync("why/abc.txt", ms);
            await client.UploadAsync("why1/abc.txt", ms);

            var strings = await client.GetDirectoriesAsync(start: 1);
            Assert.Equal(new[] { "/why", "/why1" }, strings);
        }

        [Fact]
        public void CanDetectRemoval()
        {
            var client = NewAsyncClient();
            var ms = new MemoryStream();
            client.UploadAsync("test/abc.txt", ms).Wait();
            client.UploadAsync("test/ced.txt", ms).Wait();
            client.UploadAsync("why/abc.txt", ms).Wait();
            client.UploadAsync("why1/abc.txt", ms).Wait();

            client.DeleteAsync("why1/abc.txt").Wait();

            var strings = client.GetDirectoriesAsync().Result;
            Assert.Equal(new[] { "/test", "/why" }, strings);
        }

        [Fact]
        public async Task Should_not_see_already_deleted_files()
        {
            var client = NewAsyncClient();
            var ms = new MemoryStream();
            client.UploadAsync("visible.bin", ms).Wait();
            client.UploadAsync("toDelete.bin", ms).Wait();

            client.DeleteAsync("toDelete.bin").Wait();

            var results = await client.SearchOnDirectoryAsync("/");
            var fileNames = results.Files.Select(x => x.Name).ToArray();
            Assert.Equal(new[] { "visible.bin" }, fileNames);
        }
    }
}
