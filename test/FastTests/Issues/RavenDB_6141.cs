using Raven.Server.Config;
using Raven.Server.Config.Settings;
using Raven.Server.ServerWide;
using Xunit;

namespace FastTests.Issues
{
    public class RavenDB_6141 : NoDisposalNeeded
    {
        [Fact]
        public void Default_database_path_settings()
        {
            var config = new RavenConfiguration("foo", ResourceType.Database);

            config.Initialize();

            Assert.Equal(new PathSetting("Databases/foo").FullPath, config.Core.DataDirectory.FullPath);
            Assert.Equal(new PathSetting("Databases/foo/Indexes").FullPath, config.Indexing.StoragePath.FullPath);

            Assert.Null(config.Indexing.TempPath);
            Assert.Null(config.Indexing.JournalsStoragePath);
            Assert.Null(config.Indexing.AdditionalStoragePaths);

            // TODO arek - add more tests, working directory etc..
        }
    }
}