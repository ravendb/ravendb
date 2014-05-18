using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.RavenFS;
using Xunit;

namespace RavenFS.Tests
{
    public class Folders : RavenFsTestBase
	{
		[Fact]
		public void CanGetListOfFolders()
		{
			var client = NewClient();
			var ms = new MemoryStream();
			client.UploadAsync("test/abc.txt", ms).Wait();
			client.UploadAsync("test/ced.txt", ms).Wait();
			client.UploadAsync("why/abc.txt", ms).Wait();

			var strings = client.GetFoldersAsync().Result;
			Assert.Equal(new[]{"/test", "/why"}, strings);
		}

		[Fact]
		public void WillNotGetNestedFolders()
		{
			var client = NewClient();
			var ms = new MemoryStream();
			client.UploadAsync("test/ab/c.txt", ms).Wait();
			client.UploadAsync("test/ce/d.txt", ms).Wait();
			client.UploadAsync("why/abc.txt", ms).Wait();

			var strings = client.GetFoldersAsync().Result;
			Assert.Equal(new[] { "/test", "/why" }, strings);
		}

		[Fact]
		public void WillNotGetOtherFolders()
		{
			var client = NewClient();
			var ms = new MemoryStream();
			client.UploadAsync("test/ab/c.txt", ms).Wait();
			client.UploadAsync("test/ce/d.txt", ms).Wait();
			client.UploadAsync("test/ab/a/c.txt", ms).Wait();

			var strings = client.GetFoldersAsync("/test").Result;
			Assert.Equal(new[] {"/test/ab", "/test/ce" }, strings);
	
		}


		[Fact]
		public void CanRename()
		{
			var client = NewClient();
			var ms = new MemoryStream();
			client.UploadAsync("test/abc.txt", ms).Wait();

			client.RenameAsync("test/abc.txt", "test2/abc.txt").Wait();

			client.DownloadAsync("test2/abc.txt", new MemoryStream()).Wait();// would thorw if missing
		}



		[Fact]
		public void AfterRename_OldFolderIsGoneAndWeHaveNewOne()
		{
			var client = NewClient();
			var ms = new MemoryStream();
			client.UploadAsync("test/abc.txt", ms).Wait();

			Assert.Contains("/test", client.GetFoldersAsync().Result);

			client.RenameAsync("test/abc.txt", "test2/abc.txt").Wait();

			client.DownloadAsync("test2/abc.txt", new MemoryStream()).Wait();// would thorw if missing

			Assert.DoesNotContain("/test", client.GetFoldersAsync().Result);

			Assert.Contains("/test2", client.GetFoldersAsync().Result);

		}

		[Fact]
		public async Task CanGetListOfFilesInFolder()
		{
			var client = NewClient();
			var ms = new MemoryStream();
			await client.UploadAsync("test/abc.txt", ms);
			await client.UploadAsync("test/ced.txt", ms);
			await client.UploadAsync("why/abc.txt", ms);

			var results = await client.GetFilesAsync("/test");
			var strings = results.Files.Select(x => x.Name).ToArray();
			Assert.Equal(new[] { "/test/abc.txt", "/test/ced.txt" }, strings);
		}


		[Fact]
		public void CanGetListOfFilesInFolder_Sorted_Size()
		{
			var client = NewClient();
			client.UploadAsync("test/abc.txt", new MemoryStream(new byte[4])).Wait();
			client.UploadAsync("test/ced.txt", new MemoryStream(new byte[8])).Wait();

			var strings = client.GetFilesAsync("/test", FilesSortOptions.Size | FilesSortOptions.Desc).Result.Files.Select(x => x.Name).ToArray();
			Assert.Equal(new[] { "/test/ced.txt", "/test/abc.txt" }, strings);
		}

		[Fact]
		public void CanGetListOfFilesInFolder_Sorted_Name()
		{
			var client = NewClient();
			client.UploadAsync("test/abc.txt", new MemoryStream(new byte[4])).Wait();
			client.UploadAsync("test/ced.txt", new MemoryStream(new byte[8])).Wait();

			var strings = client.GetFilesAsync("/test", FilesSortOptions.Name | FilesSortOptions.Desc).Result.Files.Select(x => x.Name).ToArray();
			Assert.Equal(new[] { "/test/ced.txt", "/test/abc.txt" }, strings);
		}


		[Fact]
		public void CanGetListOfFilesInFolderInRoot()
		{
			var client = NewClient();
			var ms = new MemoryStream();
			client.UploadAsync("test/abc.txt", ms).Wait();
			client.UploadAsync("test/ced.txt", ms).Wait();
			client.UploadAsync("why/abc.txt", ms).Wait();

			var strings = client.GetFilesAsync("/").Result.Files.Select(x => x.Name).ToArray();
			Assert.Equal(new string[] { }, strings);
		}


		[Fact]
		public void CanGetListOfFilesInFolder2()
		{
			var client = NewClient();
			var ms = new MemoryStream();
			client.UploadAsync("test/abc.txt", ms).Wait();
			client.UploadAsync("test/ced.txt", ms).Wait();
			client.UploadAsync("why/abc.txt", ms).Wait();

			var strings = client.GetFilesAsync("/test").Result.Files.Select(x => x.Name).ToArray();
			Assert.Equal(new string[] { "/test/abc.txt", "/test/ced.txt" }, strings);
		}

        [Fact]
        public void CanSearchForFilesByPattern()
        {
            var client = NewClient();
            var ms = new MemoryStream();

            client.UploadAsync("abc.txt", ms).Wait();
            client.UploadAsync("def.txt", ms).Wait();
            client.UploadAsync("dhi.txt", ms).Wait();

            var fileNames =
                client.GetFilesAsync("/", fileNameSearchPattern: "d*").Result.Files.Select(x => x.Name).ToArray();
            Assert.Equal(new string[] { "def.txt", "dhi.txt"}, fileNames);
        }

        [Fact]
        public void CanSearchForFilesByPatternBeginningWithMultiCharacterWildcard()
        {
            var client = NewClient();
            var ms = new MemoryStream();

            client.UploadAsync("abc.txt", ms).Wait();
            client.UploadAsync("def.txt", ms).Wait();
            client.UploadAsync("ghi.png", ms).Wait();
            client.UploadAsync("jkl.png", ms).Wait();

            var fileNames =
                client.GetFilesAsync("/", fileNameSearchPattern: "*.png").Result.Files.Select(x => x.Name).ToArray();
            Assert.Equal(new string[] { "ghi.png", "jkl.png" }, fileNames);
        }

        [Fact]
        public void CanSearchForFilesByPatternBeginningWithSingleCharacterWildcard()
        {
            var client = NewClient();
            var ms = new MemoryStream();

            client.UploadAsync("abc.txt", ms).Wait();
            client.UploadAsync("def.txt", ms).Wait();
            client.UploadAsync("ghi.png", ms).Wait();
            client.UploadAsync("jkl.png", ms).Wait();

            var fileNames =
                client.GetFilesAsync("/", fileNameSearchPattern: "?bc?txt").Result.Files.Select(x => x.Name).ToArray();
            Assert.Equal(new string[] { "abc.txt" }, fileNames);
        }

        [Fact]
        public void CanSearchForFilesByNameWithinAFolder()
        {
            var client = NewClient();
            var ms = new MemoryStream();

            client.UploadAsync("test/abc.txt", ms).Wait();
            client.UploadAsync("test/def.txt", ms).Wait();

            var fileNames =
                client.GetFilesAsync("/test", fileNameSearchPattern: "a*").Result.Files.Select(x => x.Name).ToArray();
            Assert.Equal(new string[] { "/test/abc.txt" }, fileNames);
        }

        [Fact]
        public void CanSearchPatternContainingNoWildcardsDoesStartsWithSearchByDefault()
        {
            var client = NewClient();
            var ms = new MemoryStream();

            client.UploadAsync("abc.txt", ms).Wait();
            client.UploadAsync("def.txt", ms).Wait();

            var fileNames =
                client.GetFilesAsync("/", fileNameSearchPattern: "a").Result.Files.Select(x => x.Name).ToArray();
            Assert.Equal(new string[] { "abc.txt" }, fileNames);
        }

		[Fact]
		public void CanPage()
		{
			var client = NewClient();
			var ms = new MemoryStream();
			client.UploadAsync("test/abc.txt", ms).Wait();
			client.UploadAsync("test/ced.txt", ms).Wait();
			client.UploadAsync("why/abc.txt", ms).Wait();
			client.UploadAsync("why1/abc.txt", ms).Wait();

			var strings = client.GetFoldersAsync(start: 1).Result;
			Assert.Equal(new[] { "/why", "/why1" }, strings);
		}

		[Fact]
		public void CanDetectRemoval()
		{
			var client = NewClient();
			var ms = new MemoryStream();
			client.UploadAsync("test/abc.txt", ms).Wait();
			client.UploadAsync("test/ced.txt", ms).Wait();
			client.UploadAsync("why/abc.txt", ms).Wait();
			client.UploadAsync("why1/abc.txt", ms).Wait();

			client.DeleteAsync("why1/abc.txt").Wait();

			var strings = client.GetFoldersAsync().Result;
			Assert.Equal(new[] { "/test", "/why" }, strings);
		}

		[Fact]
		public void Should_not_see_already_deleted_files()
		{
			var client = NewClient();
			var ms = new MemoryStream();
			client.UploadAsync("visible.bin", ms).Wait();
			client.UploadAsync("toDelete.bin", ms).Wait();

			client.DeleteAsync("toDelete.bin").Wait();

			var fileNames =
				client.GetFilesAsync("/").Result.Files.Select(x => x.Name).ToArray();
			Assert.Equal(new[] { "visible.bin" }, fileNames);
		}
	}
}