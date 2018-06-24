using System;
using System.Collections.Generic;
using System.IO;

namespace Raven.Embedded
{
    public class ServerOptions
    {
        public string FrameworkVersion { get; set; } = "2.1.1";

        public string DataDirectory { get; set; } = Path.Combine(AppContext.BaseDirectory, "RavenDB");

        internal string ServerDirectory { get; set; } = Path.Combine(AppContext.BaseDirectory, "RavenDBServer");

        public TimeSpan MaxServerStartupTimeDuration { get; set; } = TimeSpan.FromMinutes(1);

        public List<string> CommandLineArgs { get; set; } = new List<string>();

        internal static ServerOptions Default = new ServerOptions();
    }
}
