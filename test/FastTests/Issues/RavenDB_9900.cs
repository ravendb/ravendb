using Raven.Server.Config;
using Raven.Server.Config.Settings;
using Raven.Server.ServerWide;
using Xunit;

namespace FastTests.Issues
{
    public class RavenDB_9900 : NoDisposalNeeded
    {
        [Fact]
        public void Database_creation_using_relative_path_creates_directories_incorrectly()
        {
            var server = new RavenConfiguration(null, ResourceType.Server);

            server.SetSetting(RavenConfiguration.GetKey(x => x.Core.RunInMemory), "true");
            server.SetSetting(RavenConfiguration.GetKey(x => x.Core.DataDirectory), "RavenData");

            server.Initialize();

            var database = RavenConfiguration.CreateFrom(server, "foo", ResourceType.Database);

            database.SetSetting(RavenConfiguration.GetKey(x => x.Core.RunInMemory), "true");
            database.SetSetting(RavenConfiguration.GetKey(x => x.Core.DataDirectory), @"MyWork\MyDatabaseFolder");

            database.Initialize();

            Assert.Equal(new PathSetting("RavenData/MyWork/MyDatabaseFolder").FullPath, database.Core.DataDirectory.FullPath);
            Assert.Equal(new PathSetting("RavenData/MyWork/MyDatabaseFolder/Indexes").FullPath, database.Indexing.StoragePath.FullPath);

            Assert.Equal(new PathSetting("RavenData/MyWork/MyDatabaseFolder/Journal").FullPath, database.Core.DataDirectory.Combine("Journal").FullPath);
        }
    }
}
