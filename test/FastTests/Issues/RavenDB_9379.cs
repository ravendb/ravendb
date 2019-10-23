using System;
using Raven.Server.Config;
using Raven.Server.Utils.Cli;
using Sparrow.Platform;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Issues
{
    public class RavenDB_9379 : NoDisposalNeeded
    {
        public RavenDB_9379(ITestOutputHelper output) : base(output)
        {
        }

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
            var oldRunningInDocker = PlatformDetails.RunningOnDocker;
            const string RemoveUnsecuredCliArg = "REMOVE_UNSECURED_CLI_ARG_AFTER_RESTART";

            string[] updatedArgs;
            try
            {
                PlatformDetails.RunningOnDocker = true;
                Assert.True(PlatformDetails.RunningOnDocker, "PlatformDetails.RunningOnDocker");

                Environment.SetEnvironmentVariable(RemoveUnsecuredCliArg, "true");

                var confBeforeRestart = RavenConfiguration.CreateForServer(null);

                var newConf = RavenConfiguration.CreateForServer(null);

                // ACT
                updatedArgs = PostSetupCliArgumentsUpdater.Process(args, confBeforeRestart, newConf);
            }
            finally
            {
                PlatformDetails.RunningOnDocker = oldRunningInDocker;
                Assert.Equal(oldRunningInDocker, PlatformDetails.RunningOnDocker);
                Environment.SetEnvironmentVariable(RemoveUnsecuredCliArg, oldEnv[RemoveUnsecuredCliArg] as string);
            }

            // ASSERT
            Assert.True(updatedArgs.Length == 0);
        }
    }
}
