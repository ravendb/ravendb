using System;
using System.Configuration;
using System.IO;
using log4net.Config;

namespace Raven.Database
{
    public class RavenConfiguration
    {
        public RavenConfiguration()
        {
            var portStr = ConfigurationManager.AppSettings["RavenPort"];

            Port = portStr != null ? int.Parse(portStr) : 8080;

            var indexBatchSizeStr = ConfigurationManager.AppSettings["IndexingBatchSize"];

            IndexingBatchSize = indexBatchSizeStr != null ? int.Parse(indexBatchSizeStr) : 100;

            DataDirectory = ConfigurationManager.AppSettings["RavenDataDir"] ?? @"..\..\..\Data";

            WebDir = ConfigurationManager.AppSettings["RavenWebDir"] ??
                     Path.Combine(AppDomain.CurrentDomain.BaseDirectory, @"..\..\WebUI");

            AnonymousUserAccessMode = ConfigurationManager.AppSettings["AnonymousAccess"] != null ? 
                (AnonymousUserAccessMode)Enum.Parse(typeof(AnonymousUserAccessMode), ConfigurationManager.AppSettings["AnonymousAccess"]) : 
                AnonymousUserAccessMode.Get;
        }

        public string DataDirectory { get; set; }
        public int Port { get; set; }
        public string WebDir { get; set; }
        public int IndexingBatchSize { get; set; }
        public AnonymousUserAccessMode AnonymousUserAccessMode { get; set; }

        public void LoadLoggingSettings()
        {
            XmlConfigurator.ConfigureAndWatch(
                new FileInfo(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "log4net.config")));
        }
    }
}