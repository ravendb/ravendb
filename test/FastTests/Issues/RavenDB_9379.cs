using Raven.Server.Config;
using Raven.Server.ServerWide;
using Raven.Server.Utils.Cli;
using Xunit;

namespace FastTests.Issues
{
    public class RavenDB_9379 : NoDisposalNeeded
    {
        [Fact]
        public void ServerUrlHostAndPortCliArgShouldStayTheSameAfterRestart()
        {
            // ARRANGE
            var args = new string[]
            {
                "--ServerUrl=http://0.0.0.0:8080"
            };

            var confBeforeRestart = new RavenConfiguration(null, ResourceType.Server);
            confBeforeRestart.Core.ServerUrls = new[] {"http://0.0.0.0:8080"};

            var newConf = new RavenConfiguration(null, ResourceType.Server);
            newConf.SetSetting("ServerUrl", "http://127.0.0.1:9090");

            // ACT
            var updatedArgs = PostSetupCliArgumentsUpdater.Process(args, confBeforeRestart, newConf);

            // ASSERT
            Assert.Equal("--ServerUrl=http://0.0.0.0:8080", updatedArgs[0]);
        }

        [Fact]
        public void GivenServerUrlCliArgAndSecuredSetupSchemeShouldBeUpdatedAfterRestart()
        {
            // ARRANGE
            var args = new string[]
            {
                "--ServerUrl=http://0.0.0.0:8080"
            };

            var confBeforeRestart = new RavenConfiguration(null, ResourceType.Server);
            confBeforeRestart.Core.ServerUrls = new[] {"http://0.0.0.0:8080"};

            var newConf = new RavenConfiguration(null, ResourceType.Server);
            newConf.SetSetting("ServerUrl", "https://127.0.0.1:9090");

            // ACT
            var updatedArgs = PostSetupCliArgumentsUpdater.Process(args, confBeforeRestart, newConf);

            // ASSERT
            Assert.Equal("--ServerUrl=https://0.0.0.0:8080", updatedArgs[0]);
        }
    }
}
