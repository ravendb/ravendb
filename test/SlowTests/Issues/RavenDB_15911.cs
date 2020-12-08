using System.IO;
using FastTests.Voron;
using Raven.Server.Indexing;
using Voron;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_15911 : StorageTest
    {
        public RavenDB_15911(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void ShouldCleanupTempDirectoryOnStartup()
        {
            RequireFileBasedPager();

            var tempPath = Env.Options.TempPath.FullPath;

            string tempFile = Path.Combine(tempPath, $"dummy{StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions.TempFileExtension}");

            using (File.Create(tempFile))
            {

            }

            string buffersFile = Path.Combine(tempPath, $"dummy{StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions.BuffersFileExtension}");

            using (File.Create(buffersFile))
            {

            }

            string temIndexFile = VoronIndexOutput.GetTempFilePath(Options, "dummy");

            using (File.Create(temIndexFile))
            {

            }

            StopDatabase(shouldDisposeOptions: true);

            Options = StorageEnvironmentOptions.ForPath(DataDir);

            StartDatabase();

            Assert.False(File.Exists(tempFile));
            Assert.False(File.Exists(buffersFile));
            Assert.False(File.Exists(temIndexFile));
        }
    }
}
