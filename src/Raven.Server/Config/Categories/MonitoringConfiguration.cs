using System.Collections.Generic;
using System.ComponentModel;
using Microsoft.Extensions.Configuration;
using Raven.Server.Config.Attributes;
using Raven.Server.Config.Settings;
using Raven.Server.Monitoring.Snmp;
using Raven.Server.ServerWide;

namespace Raven.Server.Config.Categories
{
    [ConfigurationCategory(ConfigurationCategoryType.Monitoring)]
    public sealed class MonitoringConfiguration : ConfigurationCategory
    {
        [Description("A command or executable to run which will provide machine cpu usage and total machine cores to standard output. If specified, RavenDB will use this information for monitoring cpu usage.")]
        [DefaultValue(null)]
        [ConfigurationEntry("Monitoring.Cpu.Exec", ConfigurationEntryScope.ServerWideOnly)]
        public string CpuUsageMonitorExec { get; set; }

        [Description("The command line arguments for the 'Monitoring.Cpu.Exec' command or executable. The arguments must be escaped for the command line.")]
        [DefaultValue(null)]
        [ConfigurationEntry("Monitoring.Cpu.Exec.Arguments", ConfigurationEntryScope.ServerWideOnly, isSecured: true)]
        public string CpuUsageMonitorExecArguments { get; set; }

        [Description("The minimum interval between measures to calculate the disk stats")]
        [DefaultValue(1000)]
        [TimeUnit(TimeUnit.Milliseconds)]
        [ConfigurationEntry("Monitoring.Disk.ReadStatsDebounceTimeInMs", ConfigurationEntryScope.ServerWideOnly)]
        public TimeSetting MinDiskStatsInterval { get; set; }
        
        public MonitoringConfiguration()
        {
            Snmp = new SnmpConfiguration();
            OpenTelemetry = new OpenTelemetryConfiguration();
        }

        public SnmpConfiguration Snmp { get; }
        public OpenTelemetryConfiguration OpenTelemetry { get; }

        public override void Initialize(IConfigurationRoot settings, HashSet<string> settingsNames, IConfigurationRoot serverWideSettings, HashSet<string> serverWideSettingsNames, ResourceType type, string resourceName)
        {
            base.Initialize(settings, settingsNames, serverWideSettings, serverWideSettingsNames, type, resourceName);
            Snmp.Initialize(settings, settingsNames, serverWideSettings, serverWideSettingsNames, type, resourceName);
            OpenTelemetry.Initialize(settings, settingsNames, serverWideSettings, serverWideSettingsNames, type, resourceName);
            Initialized = true;
        }

        [ConfigurationCategory(ConfigurationCategoryType.Monitoring)]
        public sealed class OpenTelemetryConfiguration : ConfigurationCategory
        {
            [Description("Indicates if OpenTelemetry is enabled or not. Default: false")]
            [DefaultValue(false)]
            [ConfigurationEntry("Monitoring.OpenTelemetry.Enabled", ConfigurationEntryScope.ServerWideOnly)]
            public bool Enabled { get; set; }
            
            [Description("Indicates if server-wide OpenTelemetry is enabled or not. Default: true")]
            [DefaultValue(true)]
            [ConfigurationEntry("Monitoring.OpenTelemetry.ServerWide.Enabled", ConfigurationEntryScope.ServerWideOnly)]
            public bool ServerWideEnabled { get; set; }
            
            [Description("Indicates if AspNetCoreInstrumentation metrics are enabled or not. Default: true")]
            [DefaultValue(true)]
            [ConfigurationEntry("Monitoring.OpenTelemetry.Metrics.AspNetCoreInstrumentation.Enabled", ConfigurationEntryScope.ServerWideOnly)]
            public bool AspNetCoreInstrumentationMetricsEnabled { get; set; }
            
            [Description("Indicates if AspNetCoreInstrumentation metrics are enabled or not. Default: true")]
            [DefaultValue(true)]
            [ConfigurationEntry("Monitoring.OpenTelemetry.Metrics.RuntimeInstrumentation.Enabled", ConfigurationEntryScope.ServerWideOnly)]
            public bool RuntimeInstrumentationMetricsEnabled { get; set; }
            
            [Description("Indicates if should use OpenTelementry protocol.")]
            [DefaultValue(true)]
            [ConfigurationEntry("Monitoring.OpenTelemetry.OpenTelemetryProtocol.Enabled", ConfigurationEntryScope.ServerWideOnly)]
            public bool OpenTelemetryProtocolExporter { get; set; }
            
            [Description("Indicates meters should be exposed to console.")]
            [DefaultValue(false)]
            [ConfigurationEntry("Monitoring.OpenTelemetry.ConsoleExporter", ConfigurationEntryScope.ServerWideOnly)]
            public bool ConsoleExporter { get; set; }
            
            [Description("Expose server storage instruments.")]
            [DefaultValue(true)]
            [ConfigurationEntry("Monitoring.OpenTelemetry.ServerWide.Storage.Enabled", ConfigurationEntryScope.ServerWideOnly)]
            public bool ServerStorage { get; set; }
            
            [Description("Expose instruments related to CPU credits.")]
            [DefaultValue(true)]
            [ConfigurationEntry("Monitoring.OpenTelemetry.ServerWide.CPUCredits.Enabled", ConfigurationEntryScope.ServerWideOnly)]
            public bool CPUCredits { get; set; }
            
            [Description("Expose instruments related to hardware usage.")]
            [DefaultValue(true)]
            [ConfigurationEntry("Monitoring.OpenTelemetry.ServerWide.Hardware.Enabled", ConfigurationEntryScope.ServerWideOnly)]
            public bool Hardware { get; set; }

            [Description("Expose instruments related to aggregated database statistics.")]
            [DefaultValue(true)]
            [ConfigurationEntry("Monitoring.OpenTelemetry.ServerWide.TotalDatabases.Enabled", ConfigurationEntryScope.ServerWideOnly)]
            public bool TotalDatabases { get; set; }
            
            [Description("Expose instruments related to requests.")]
            [DefaultValue(true)]
            [ConfigurationEntry("Monitoring.OpenTelemetry.ServerWide.Requests.Enabled", ConfigurationEntryScope.ServerWideOnly)]
            public bool Requests { get; set; }
            
            [Description("Expose instruments related to GC.")]
            [DefaultValue(true)]
            [ConfigurationEntry("Monitoring.OpenTelemetry.ServerWide.GC.Enabled", ConfigurationEntryScope.ServerWideOnly)]
            public bool GcEnabled { get; set; }
        }

        [ConfigurationCategory(ConfigurationCategoryType.Monitoring)]
        public sealed class SnmpConfiguration : ConfigurationCategory
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
            [ConfigurationEntry("Monitoring.Snmp.AuthenticationPassword.Secondary", ConfigurationEntryScope.ServerWideOnly, isSecured: true)]
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
