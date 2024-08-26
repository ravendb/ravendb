using System.IO;
using FastTests;
using Raven.Server.Indexing;
using Voron;
using Voron.Util.Settings;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Tests
{
    public class TempFileCacheTests : RavenTestBase
    {
        public TempFileCacheTests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void Can_reuse_files_for_cache()
        {
            var path = new VoronPathSetting(NewDataPath());
            var environment = new StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions(path, path, path, null, null, null, null);

            using (File.Create(TempFileCache.GetTempFileName(environment)))
            {
            }

            using (var cache = new TempFileCache(environment))
            {
                Assert.Equal(1, cache.FilesCount);
            }
        }

        [Fact]
        public void Skip_files_that_are_in_use()
        {
            var path = new VoronPathSetting(NewDataPath());
            var environment = new StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions(path, path, path, null, null, null, null);

            for (var i = 0; i < TempFileCache.MaxFilesToKeepInCache; i++)
            {
                using (File.Create(TempFileCache.GetTempFileName(environment)))
                {
                }
            }

            using (File.Create(Path.Combine(environment.TempPath.FullPath,
                TempFileCache.FilePrefix + "Z" + StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions.TempFileExtension)))
            {
                using (new TempFileCache(environment))
                {
                }
            }
        }
    }
}
