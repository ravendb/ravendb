using System.IO;
using Raven.TestDriver;
using Sparrow.Platform;

namespace Tests.Infrastructure.InterversionTest
{
    public class ConfigurableRavenServerLocator : RavenServerLocator
    {
        private readonly string _serverDirPath;

        public ConfigurableRavenServerLocator(string serverDirPath)
        {
            _serverDirPath = serverDirPath;
        }

        public override string CommandArguments => "--Http.UseLibuv=true";

        public override string ServerPath => Path.Combine(
            _serverDirPath,
            "Server",
            PlatformDetails.RunningOnPosix ? "Raven.Server" : "Raven.Server.exe");
    }
}
