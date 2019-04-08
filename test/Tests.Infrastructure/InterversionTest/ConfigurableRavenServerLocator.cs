using System.IO;
using Sparrow.Platform;

namespace Tests.Infrastructure.InterversionTest
{
    public class ConfigurableRavenServerLocator : RavenServerLocator
    {
        private readonly string _serverDirPath;

        public ConfigurableRavenServerLocator(string serverDirPath, bool includeLibuvArg = true)
        {
            _serverDirPath = serverDirPath;
            if (includeLibuvArg == false)
                SetCommandArguments();
        }

        private void SetCommandArguments()
        {
            CommandArguments = base.CommandArguments;
        }

        public override string CommandArguments { get; set; } = "--Http.UseLibuv = true";


        public override string ServerPath => Path.Combine(
            _serverDirPath,
            "Server",
            PlatformDetails.RunningOnPosix ? "Raven.Server" : "Raven.Server.exe");
    }
}
