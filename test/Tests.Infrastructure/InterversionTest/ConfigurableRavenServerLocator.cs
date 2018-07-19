using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Raven.TestDriver;
using Sparrow.Platform;
using Tests.Infrastructure.InterversionTest;

namespace Tests.Infrastructure
{
    public class ConfigurableRavenServerLocator : RavenServerLocator
    {
        private readonly string _serverDirPath;

        public ConfigurableRavenServerLocator(string serverDirPath)
        {
            _serverDirPath = serverDirPath;
        }

        public override string ServerPath
        {
            get
            {
                return Path.Combine(
                    _serverDirPath, 
                    "Server", 
                    PlatformDetails.RunningOnPosix ? "Raven.Server" : "Raven.Server.exe");
            }
        }

    }
}
