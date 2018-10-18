using System;
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

            var confBeforeRestart = RavenConfiguration.CreateForServer(null);
            confBeforeRestart.Core.ServerUrls = new[] {"http://0.0.0.0:8080"};

            var newConf = RavenConfiguration.CreateForServer(null);
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

            var confBeforeRestart = RavenConfiguration.CreateForServer(null);
            confBeforeRestart.Core.ServerUrls = new[] {"http://0.0.0.0:8080"};

            var newConf = RavenConfiguration.CreateForServer(null);
            newConf.SetSetting("ServerUrl", "https://127.0.0.1:9090");

            // ACT
            var updatedArgs = PostSetupCliArgumentsUpdater.Process(args, confBeforeRestart, newConf);

            // ASSERT
            Assert.Equal("--ServerUrl=https://0.0.0.0:8080", updatedArgs[0]);
        }

        [Fact]
        public void GivenUnsecuredAccessSetInCliArgsRemoveItAfterSetupIfRunOnDocker()
        {
            // ARRANGE
            var args = new string[]
            {
                "--Security.UnsecuredAccessAllowed=PublicNetwork"
            };

            var oldEnv = Environment.GetEnvironmentVariables();
            const string RavenInDocker = "RAVEN_IN_DOCKER";
            const string RemoveUnsecuredCliArg = "REMOVE_UNSECURED_CLI_ARG_AFTER_RESTART";

            string[] updatedArgs;
            try
            {

                Environment.SetEnvironmentVariable(RavenInDocker, "true");
                Environment.SetEnvironmentVariable(RemoveUnsecuredCliArg, "true");

                var confBeforeRestart = RavenConfiguration.CreateForServer(null);

                var newConf = RavenConfiguration.CreateForServer(null);

                // ACT
                updatedArgs = PostSetupCliArgumentsUpdater.Process(args, confBeforeRestart, newConf);
            }
            finally
            {
                Environment.SetEnvironmentVariable(RavenInDocker, oldEnv[RavenInDocker] as string);
                Environment.SetEnvironmentVariable(RemoveUnsecuredCliArg, oldEnv[RemoveUnsecuredCliArg] as string);
            }

            // ASSERT
            Assert.True(updatedArgs.Length == 0);
        }
    }
}
