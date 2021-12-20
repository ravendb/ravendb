using System.IO;
using System.Threading;
using Sparrow.Platform;

namespace Tests.Infrastructure.InterversionTest
{
    public class ConfigurableRavenServerLocator : RavenServerLocator
    {
        private readonly string _serverDirPath;
        private readonly string _dataDir;
        private readonly string _serverUrl;
        private readonly string _commandsArg;
        private static int _port = 8080;
        private static int Port => Interlocked.Increment(ref _port) - 1;

        public ConfigurableRavenServerLocator(string serverDirPath, string version, string dataDir = null, string url = null)
        {
            _serverDirPath = serverDirPath;
            _dataDir = dataDir;
            _serverUrl = url;
            if (version.StartsWith("4."))
            {
                _commandsArg = "--Http.UseLibuv=true";
            }
            if (version.StartsWith("4.0") == false)
            {
                _commandsArg += " --Features.Availability=Experimental";
            }
        }

        public override string CommandArguments => _commandsArg;

        public override string ServerPath => Path.Combine(
            _serverDirPath,
            PlatformDetails.RunningOnPosix ? "Raven.Server" : "Raven.Server.exe");

        public override string ServerUrl => _serverUrl ?? $"http://127.0.0.1:{Port}";
        public override string DataDir => _dataDir ?? _serverDirPath;
    }
}
