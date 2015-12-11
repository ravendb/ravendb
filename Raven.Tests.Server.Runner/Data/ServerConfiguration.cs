using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;

using Raven.Database.Config;

namespace Raven.Tests.Server.Runner.Data
{
    [Serializable]
    public class ServerConfiguration
    {
        public ServerConfiguration()
        {
            Settings = new Dictionary<string, string>();
        }

        public int Port { get; set; }

        public bool RunInMemory { get; set; }

        public bool UseCommercialLicense { get; set; }

        public string ApiKeyName { get; set; }

        public string ApiKeySecret { get; set; }

        public IDictionary<string, string> Settings { get; set; }

        public bool HasApiKey { get { return !string.IsNullOrEmpty(ApiKeyName) && !string.IsNullOrEmpty(ApiKeySecret); } }

        public RavenConfiguration ConvertToRavenConfiguration()
        {
            var configuration = new RavenConfiguration();

            foreach (var p in ConfigurationManager.AppSettings.AllKeys.Select(k => Tuple.Create(k, ConfigurationManager.AppSettings[k])))
            {
                configuration.SetSetting(p.Item1, p.Item2);
            }

            foreach (var key in Settings.Keys)
            {
                configuration.SetSetting(key, Settings[key]);
            }

            configuration.Initialize();

            configuration.Core.Port = Port;
            configuration.Core.RunInMemory = RunInMemory;

            return configuration;
        }
    }
}
