using System.Collections.Generic;
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

        // skips ports another process already listens on (e.g. a server the developer runs on 8080)
        private static int Port
        {
            get
            {
                while (true)
                {
                    var port = Interlocked.Increment(ref _port) - 1;
                    if (IsPortFree(port))
                        return port;
                }
            }
        }

        private static bool IsPortFree(int port)
        {
            try
            {
                var listener = new System.Net.Sockets.TcpListener(System.Net.IPAddress.Loopback, port);
                listener.Start();
                listener.Stop();
                return true;
            }
            catch (System.Net.Sockets.SocketException)
            {
                return false;
            }
        }

        public ConfigurableRavenServerLocator(string serverDirPath, string version, string dataDir = null, string url = null, Dictionary<string, string> environmentVariables = null, Dictionary<string, string> customSettings = null)
        {
            _serverDirPath = serverDirPath;
            _dataDir = dataDir;
            _serverUrl = url;
            EnvironmentVariables = environmentVariables;
            if (version.StartsWith("4."))
            {
                _commandsArg = "--Http.UseLibuv=true";
            }
            if (version.StartsWith("4.0") == false)
            {
                _commandsArg += " --Features.Availability=Experimental";
            }

            if (customSettings != null)
            {
                foreach (var customSetting in customSettings)
                    _commandsArg += $" --{customSetting.Key}={customSetting.Value}";
            }
        }

        public override string CommandArguments => _commandsArg;
        public override Dictionary<string, string> EnvironmentVariables { get; }

        public override string ServerPath => Path.Combine(
            _serverDirPath,
            PlatformDetails.RunningOnPosix ? "Raven.Server" : "Raven.Server.exe");

        public override string ServerUrl => _serverUrl ?? $"http://127.0.0.1:{Port}";
        public override string DataDir => _dataDir ?? _serverDirPath;
    }
}
