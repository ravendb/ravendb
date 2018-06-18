using System;
using System.Collections.Generic;
using System.IO;

namespace Raven.Embedded
{
    public class ServerOptions
    {
        public string FrameworkVersion { get; set; } = "2.1.0";

        public string DataDir { get; set; } = Path.Combine(AppContext.BaseDirectory, "RavenDB");

        public TimeSpan MaxServerStartupTimeDuration { get; set; } = TimeSpan.FromMinutes(1);

        public List<string> CommandLineArgs { get; set; } = new List<string>();

        internal static ServerOptions Default = new ServerOptions();
    }
}
