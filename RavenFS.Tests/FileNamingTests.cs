using Raven.Database.Server.RavenFS.Util;
using Xunit;

namespace RavenFS.Tests
{
	public class FileNamingTests
	{
		[Fact]
		public void Should_be_leading_slash_if_file_is_contained_in_subdirectory()
		{
			var filename = "documents/files/file.bin";

			Assert.Equal("/documents/files/file.bin", RavenFileNameHelper.RavenPath(filename));

			filename = "/documents/files/file.bin";

			Assert.Equal("/documents/files/file.bin", RavenFileNameHelper.RavenPath(filename));
		}

		[Fact]
		public void Should_not_be_leading_slash_if_file_exists_in_main_directory()
		{
			var filename = "file.bin";

			Assert.Equal("file.bin", RavenFileNameHelper.RavenPath(filename));

			filename = "/file.bin";

			Assert.Equal("file.bin", RavenFileNameHelper.RavenPath(filename));
		}
	}
}