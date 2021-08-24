using System.Collections.Generic;
using System.ComponentModel;
using Microsoft.Extensions.Configuration;
using Raven.Server.Config.Attributes;
using Raven.Server.Monitoring.Snmp;
using Raven.Server.ServerWide;

namespace Raven.Server.Config.Categories
{
    [ConfigurationCategory(ConfigurationCategoryType.Monitoring)]
    public class MonitoringConfiguration : ConfigurationCategory
    {
        [Description("A command or executable to run which will provide machine cpu usage and total machine cores to standard output. If specified, RavenDB will use this information for monitoring cpu usage.")]
        [DefaultValue(null)]
        [ConfigurationEntry("Monitoring.Cpu.Exec", ConfigurationEntryScope.ServerWideOnly)]
        public string CpuUsageMonitorExec { get; set; }

        [Description("The command line arguments for the 'Monitoring.Cpu.Exec' command or executable. The arguments must be escaped for the command line.")]
        [DefaultValue(null)]
        [ConfigurationEntry("Monitoring.Cpu.Exec.Arguments", ConfigurationEntryScope.ServerWideOnly, isSecured: true)]
        public string CpuUsageMonitorExecArguments { get; set; }

        public MonitoringConfiguration()
        {
            Snmp = new SnmpConfiguration();
        }

        public SnmpConfiguration Snmp { get; }

        public override void Initialize(IConfigurationRoot settings, HashSet<string> settingsNames, IConfigurationRoot serverWideSettings, HashSet<string> serverWideSettingsNames, ResourceType type, string resourceName)
        {
            base.Initialize(settings, settingsNames, serverWideSettings, serverWideSettingsNames, type, resourceName);
            Snmp.Initialize(settings, settingsNames, serverWideSettings, serverWideSettingsNames, type, resourceName);

            Initialized = true;
        }

        public class SnmpConfiguration : ConfigurationCategory
        {
            [Description("Indicates if SNMP is enabled or not. Default: false")]
            [DefaultValue(false)]
            [ConfigurationEntry("Monitoring.Snmp.Enabled", ConfigurationEntryScope.ServerWideOnly)]
            public bool Enabled { get; set; }

            [Description("Port on which SNMP is listening. Default: 161")]
            [DefaultValue(161)]
            [ConfigurationEntry("Monitoring.Snmp.Port", ConfigurationEntryScope.ServerWideOnly)]
            public int Port { get; set; }

            [Description("Community string used for SNMP v2c authentication. Default: ravendb")]
            [DefaultValue("ravendb")]
            [ConfigurationEntry("Monitoring.Snmp.Community", ConfigurationEntryScope.ServerWideOnly, isSecured: true)]
            public string Community { get; set; }

            [Description("Authentication protocol used for SNMP v3 authentication. Default: SHA1")]
            [DefaultValue(SnmpAuthenticationProtocol.SHA1)]
            [ConfigurationEntry("Monitoring.Snmp.AuthenticationProtocol", ConfigurationEntryScope.ServerWideOnly)]
            public SnmpAuthenticationProtocol AuthenticationProtocol { get; set; }

            [Description("Authentication protocol used by secondary user for SNMP v3 authentication. Default: SHA1")]
            [DefaultValue(SnmpAuthenticationProtocol.SHA1)]
            [ConfigurationEntry("Monitoring.Snmp.AuthenticationProtocol.Secondary", ConfigurationEntryScope.ServerWideOnly)]
            public SnmpAuthenticationProtocol AuthenticationProtocolSecondary { get; set; }

            [Description("Authentication user used for SNMP v3 authentication. Default: ravendb")]
            [DefaultValue("ravendb")]
            [ConfigurationEntry("Monitoring.Snmp.AuthenticationUser", ConfigurationEntryScope.ServerWideOnly)]
            public string AuthenticationUser { get; set; }

            [Description("Authentication secondary user used for SNMP v3 authentication. Default: null (disabled)")]
            [DefaultValue(null)]
            [ConfigurationEntry("Monitoring.Snmp.AuthenticationUser.Secondary", ConfigurationEntryScope.ServerWideOnly)]
            public string AuthenticationUserSecondary { get; set; }

            [Description("Authentication password used for SNMP v3 authentication. If null value from 'Monitoring.Snmp.Community' is used. Default: null")]
            [DefaultValue(null)]
            [ConfigurationEntry("Monitoring.Snmp.AuthenticationPassword", ConfigurationEntryScope.ServerWideOnly, isSecured: true)]
            public string AuthenticationPassword { get; set; }

            [Description("Authentication password used by secondary user for SNMP v3 authentication.")]
            [DefaultValue(null)]
            [ConfigurationEntry("Monitoring.Snmp.AuthenticationPassword.Secondary", ConfigurationEntryScope.ServerWideOnly)]
            public string AuthenticationPasswordSecondary { get; set; }

            [Description("Privacy protocol used for SNMP v3 privacy. Default: None")]
            [DefaultValue(SnmpPrivacyProtocol.None)]
            [ConfigurationEntry("Monitoring.Snmp.PrivacyProtocol", ConfigurationEntryScope.ServerWideOnly)]
            public SnmpPrivacyProtocol PrivacyProtocol { get; set; }

            [Description("Privacy protocol used by secondary user for SNMP v3 privacy. Default: None")]
            [DefaultValue(SnmpPrivacyProtocol.None)]
            [ConfigurationEntry("Monitoring.Snmp.PrivacyProtocol.Secondary", ConfigurationEntryScope.ServerWideOnly)]
            public SnmpPrivacyProtocol PrivacyProtocolSecondary { get; set; }

            [Description("Privacy password used for SNMP v3 privacy. Default: ravendb")]
            [DefaultValue("ravendb")]
            [ConfigurationEntry("Monitoring.Snmp.PrivacyPassword", ConfigurationEntryScope.ServerWideOnly, isSecured: true)]
            public string PrivacyPassword { get; set; }

            [Description("Privacy password used by secondary user for SNMP v3 privacy.")]
            [DefaultValue(null)]
            [ConfigurationEntry("Monitoring.Snmp.PrivacyPassword.Secondary", ConfigurationEntryScope.ServerWideOnly)]
            public string PrivacyPasswordSecondary { get; set; }

            [Description("List of supported SNMP versions. Values must be semicolon separated. Default: V2C;V3")]
            [DefaultValue("V2C;V3")]
            [ConfigurationEntry("Monitoring.Snmp.SupportedVersions", ConfigurationEntryScope.ServerWideOnly)]
            public string[] SupportedVersions { get; set; }
            
            [Description("EXPERT: Disables time window checks, which are problematic for some SNMP engines. Default: false")]
            [DefaultValue(false)]
            [ConfigurationEntry("Monitoring.Snmp.DisableTimeWindowChecks", ConfigurationEntryScope.ServerWideOnly)]
            public bool DisableTimeWindowChecks { get; set; }
        }
    }
}
