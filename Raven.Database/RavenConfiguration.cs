using System;
using System.ComponentModel.Composition.Hosting;
using System.Configuration;
using System.IO;

namespace Raven.Database
{
    public class RavenConfiguration : InMemroyRavenConfiguration
    {
        public RavenConfiguration()
        {
            foreach (string setting in ConfigurationManager.AppSettings)
            {
                if (setting.StartsWith("Raven/", StringComparison.InvariantCultureIgnoreCase))
                    Settings[setting] = ConfigurationManager.AppSettings[setting];
            }

            Initialize();
        }

        
    }
}