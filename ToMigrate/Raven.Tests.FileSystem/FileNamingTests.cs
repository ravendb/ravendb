using Raven.Abstractions.FileSystem;
using Xunit;

namespace Raven.Tests.FileSystem
{
    public class FileNamingTests
    {
        [Fact]
        public void Should_be_leading_slash_if_file_is_contained_in_subdirectory()
        {
            var filename = "documents/files/file.bin";

            Assert.Equal("/documents/files/file.bin", FileHeader.Canonize(filename));

            filename = "/documents/files/file.bin";

            Assert.Equal("/documents/files/file.bin", FileHeader.Canonize(filename));
        }

        [Fact]
        public void Should_be_leading_slash_if_file_exists_in_main_directory()
        {
            var filename = "file.bin";

            Assert.Equal("/file.bin", FileHeader.Canonize(filename));

            filename = "/file.bin";

            Assert.Equal("/file.bin", FileHeader.Canonize(filename));
        }
    }
}
