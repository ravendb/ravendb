//-----------------------------------------------------------------------
// <copyright file="RavenConfiguration.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
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