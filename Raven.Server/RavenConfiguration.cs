using System;
using System.Configuration;
using System.IO;
using log4net.Config;

namespace Raven.Server
{
    public class RavenConfiguration
    {
        public RavenConfiguration()
        {
            var portStr = ConfigurationManager.AppSettings["RavenPort"];

            Port = portStr != null ? int.Parse(portStr) : 8080;

            DataDirectory = ConfigurationManager.AppSettings["RavenDataDir"] ?? @"..\..\..\Data";

            WebDir = ConfigurationManager.AppSettings["RavenWebDir"] ??
                     Path.Combine(AppDomain.CurrentDomain.BaseDirectory, @"..\..\WebUI");
        }

        public string DataDirectory { get; set; }
        public int Port { get; set; }
        public string WebDir { get; set; }

        public void LoadLoggingSettings()
        {
            XmlConfigurator.ConfigureAndWatch(
                new FileInfo(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "log4net.config")));
        }
    }
}