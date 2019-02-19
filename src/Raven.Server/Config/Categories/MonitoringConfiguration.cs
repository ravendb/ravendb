using System.ComponentModel;
using Microsoft.Extensions.Configuration;
using Raven.Server.Config.Attributes;
using Raven.Server.ServerWide;

namespace Raven.Server.Config.Categories
{
    public class MonitoringConfiguration : ConfigurationCategory
    {
        [Description("A command or executable to run which will provide machine cpu usage and total machine cores to standard output. If specified, RavenDB will use this information for monitoring cpu usage.")]
        [DefaultValue(null)]
        [ConfigurationEntry("Monitoring.Cpu.Exec", ConfigurationEntryScope.ServerWideOnly)]
        public string CpuUsageMonitorExec { get; set; }

        [Description("The command line arguments for the 'Monitoring.Cpu.Exec' command or executable. The arguments must be escaped for the command line.")]
        [DefaultValue(null)]
        [ConfigurationEntry("Monitoring.Cpu.Exec.Arguments", ConfigurationEntryScope.ServerWideOnly)]
        public string CpuUsageMonitorExecArguments { get; set; }

        public MonitoringConfiguration()
        {
            Snmp = new SnmpConfiguration();
        }

        public SnmpConfiguration Snmp { get; }

        public override void Initialize(IConfigurationRoot settings, IConfigurationRoot serverWideSettings, ResourceType type, string resourceName)
        {
            base.Initialize(settings, serverWideSettings, type, resourceName);
            Snmp.Initialize(settings, serverWideSettings, type, resourceName);

            Initialized = true;
        }

        public class SnmpConfiguration : ConfigurationCategory
        {
            [DefaultValue(false)]
            [ConfigurationEntry("Monitoring.Snmp.Enabled", ConfigurationEntryScope.ServerWideOnly)]
            public bool Enabled { get; set; }

            [DefaultValue(161)]
            [ConfigurationEntry("Monitoring.Snmp.Port", ConfigurationEntryScope.ServerWideOnly)]
            public int Port { get; set; }

            [DefaultValue("ravendb")]
            [ConfigurationEntry("Monitoring.Snmp.Community", ConfigurationEntryScope.ServerWideOnly)]
            public string Community { get; set; }
        }
    }
}
