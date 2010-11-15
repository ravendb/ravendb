using System;
using System.Configuration;

namespace Raven.Database.Config
{
    public class RavenConfiguration : InMemoryRavenConfiguration
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