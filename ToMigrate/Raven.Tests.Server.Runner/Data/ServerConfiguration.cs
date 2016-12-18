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

        public string DefaultStorageTypeName { get; set; }

        public bool UseCommercialLicense { get; set; }

        public string ApiKeyName { get; set; }

        public string ApiKeySecret { get; set; }

        public IDictionary<string, string> Settings { get; set; }

        public bool HasApiKey { get { return !string.IsNullOrEmpty(ApiKeyName) && !string.IsNullOrEmpty(ApiKeySecret); } }

        public InMemoryRavenConfiguration ConvertToRavenConfiguration()
        {
            var configuration = new InMemoryRavenConfiguration();

            foreach (var p in ConfigurationManager.AppSettings.AllKeys.Select(k => Tuple.Create(k, ConfigurationManager.AppSettings[k])))
            {
                configuration.Settings.Add(p.Item1, p.Item2);
            }

            foreach (var key in Settings.Keys)
            {
                configuration.Settings.Add(key, Settings[key]);
            }

            configuration.Initialize();

            configuration.Port = Port;
            configuration.RunInMemory = RunInMemory;
            configuration.DefaultStorageTypeName = DefaultStorageTypeName;

            return configuration;
        }
    }
}
