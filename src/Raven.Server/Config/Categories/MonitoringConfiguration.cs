using System.Collections.Specialized;
using System.ComponentModel;
using Microsoft.Extensions.Configuration;
using Raven.Server.Config.Attributes;
using Raven.Server.ServerWide;

namespace Raven.Server.Config.Categories
{
    public class MonitoringConfiguration : ConfigurationCategory
    {
        public MonitoringConfiguration()
        {
            Snmp = new SnmpConfiguration();
        }

        public SnmpConfiguration Snmp { get; }

        public override void Initialize(IConfigurationRoot settings, IConfigurationRoot serverWideSettings, ResourceType type, string resourceName)
        {
            Snmp.Initialize(settings, serverWideSettings, type, resourceName);

            Initialized = true;
        }

        public class SnmpConfiguration : ConfigurationCategory
        {
            [DefaultValue(false)]
            [ConfigurationEntry("Raven/Monitoring/Snmp/Enabled")]
            public bool Enabled { get; set; }

            [DefaultValue(161)]
            [ConfigurationEntry("Raven/Monitoring/Snmp/Port")]
            public int Port { get; set; }

            [DefaultValue("ravendb")]
            [ConfigurationEntry("Raven/Monitoring/Snmp/Community")]
            public string Community { get; set; }
        }
    }
}