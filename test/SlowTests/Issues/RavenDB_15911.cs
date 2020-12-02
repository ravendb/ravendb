using System.IO;
using FastTests.Voron;
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

            StopDatabase(shouldDisposeOptions: true);

            string filePath = Path.Combine(tempPath, "dummy.file");

            using (File.Create(filePath))
            {

            }

            Options = StorageEnvironmentOptions.ForPath(DataDir);

            StartDatabase();

            Assert.False(File.Exists(filePath));
        }
    }
}
