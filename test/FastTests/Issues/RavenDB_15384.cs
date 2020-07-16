using Raven.Server.Config;
using Raven.Server.Config.Settings;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Issues
{
    public class RavenDB_15384 : NoDisposalNeeded
    {
        public RavenDB_15384(ITestOutputHelper output) : base(output)
        {
            
        }

        [Fact]
        public void Null_path_database_setting_should_default_to_server_value()
        {
            var server = RavenConfiguration.CreateForServer(null);

            server.SetSetting(RavenConfiguration.GetKey(x => x.Core.DataDirectory), "/mnt/Raven/Data");

            server.Initialize();

            var database = RavenConfiguration.CreateForDatabase(server, "foo");

            database.SetSetting(RavenConfiguration.GetKey(x => x.Core.DataDirectory), null);

            database.Initialize();

            Assert.Equal(new PathSetting("/mnt/Raven/Data/Databases/foo").FullPath, database.Core.DataDirectory.FullPath);
            Assert.Equal(new PathSetting("/mnt/Raven/Data/Databases/foo/Indexes").FullPath, database.Indexing.StoragePath.FullPath);
        }
    }
}
