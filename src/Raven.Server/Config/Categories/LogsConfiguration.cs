using System.Collections.Generic;
using System.ComponentModel;
using Microsoft.Extensions.Configuration;
using Raven.Server.Config.Attributes;
using Raven.Server.Config.Settings;
using Raven.Server.ServerWide;
using Sparrow;
using Sparrow.Logging;

namespace Raven.Server.Config.Categories
{
    [ConfigurationCategory(ConfigurationCategoryType.Logs)]
    public class LogsConfiguration : ConfigurationCategory
    {
        public override void Initialize(IConfigurationRoot settings, HashSet<string> settingsNames, IConfigurationRoot serverWideSettings, HashSet<string> serverWideSettingsNames, ResourceType type,
            string resourceName)
        {
            base.Initialize(settings, settingsNames, serverWideSettings, serverWideSettingsNames, type, resourceName);
            MicrosoftLogsPath ??= Path.Combine("MicrosoftLogs");
        }

        [DefaultValue("Logs")]
        [ConfigurationEntry("Logs.Path", ConfigurationEntryScope.ServerWideOnly)]
        public PathSetting Path { get; set; }

        [DefaultValue(LogMode.Operations)]
        [ConfigurationEntry("Logs.Mode", ConfigurationEntryScope.ServerWideOnly)]
        public LogMode Mode { get; set; }

        [DefaultValue(true)]
        [ConfigurationEntry("Logs.UseUtcTime", ConfigurationEntryScope.ServerWideOnly)]
        public bool UseUtcTime { get; set; }

        [DefaultValue(128)]
        [MinValue(16)]
        [SizeUnit(SizeUnit.Megabytes)]
        [ConfigurationEntry("Logs.MaxFileSizeInMb", ConfigurationEntryScope.ServerWideOnly)]
        public Size MaxFileSize { get; set; }

        [Description("How far back we should retain log entries in hours")]
        [DefaultValue(3 * 24)]
        [MinValue(24)]
        [TimeUnit(TimeUnit.Hours)]
        [ConfigurationEntry("Logs.RetentionTimeInHrs", ConfigurationEntryScope.ServerWideOnly)]
        public TimeSetting? RetentionTime { get; set; }

        [Description("The maximum size of the log after which the old files will be deleted")]
        [DefaultValue(null)]
        [MinValue(256)]
        [SizeUnit(SizeUnit.Megabytes)]
        [ConfigurationEntry("Logs.RetentionSizeInMb", ConfigurationEntryScope.ServerWideOnly)]
        public Size? RetentionSize { get; set; }

        [Description("Will determine whether to compress the log files")]
        [DefaultValue(false)]
        [ConfigurationEntry("Logs.Compress", ConfigurationEntryScope.ServerWideOnly)]
        public bool Compress { get; set; }

        #region Microsoft Logs
        [Description("Will determine whether to disable the Microsoft logs")]
        [DefaultValue(true)]
        [ConfigurationEntry("Logs.Microsoft.Disable", ConfigurationEntryScope.ServerWideOnly)]
        public bool DisableMicrosoftLogs { get; set; }
        
        [Description("The path to the folder where Microsoft log will be written")]
        [DefaultValue(DefaultValueSetInConstructor)]
        [ConfigurationEntry("Logs.Microsoft.Path", ConfigurationEntryScope.ServerWideOnly)]
        public PathSetting MicrosoftLogsPath { get; set; }
        
        [Description("The path to json configuration file of Microsoft logs")]
        [ReadOnlyPath]
        [DefaultValue("settings.logs.microsoft.json")]
        [ConfigurationEntry("Logs.Microsoft.ConfigurationPath", ConfigurationEntryScope.ServerWideOnly)]
        public PathSetting MicrosoftLogsConfigurationPath { get; set; }
        
        [DefaultValue(null)]
        [MinValue(16)]
        [SizeUnit(SizeUnit.Megabytes)]
        [ConfigurationEntry("Logs.Microsoft.MaxFileSizeInMb", ConfigurationEntryScope.ServerWideOnly)]
        public Size? MicrosoftLogsMaxFileSize { get; set; }

        [Description("How far back we should retain Microsoft log entries in hours")]
        [DefaultValue(null)]
        [MinValue(24)]
        [TimeUnit(TimeUnit.Hours)]
        [ConfigurationEntry("Logs.Microsoft.RetentionTimeInHrs", ConfigurationEntryScope.ServerWideOnly)]
        public TimeSetting? MicrosoftLogsRetentionTime { get; set; }

        [Description("The maximum size of the Microsoft log after which the old files will be deleted")]
        [DefaultValue(null)]
        [MinValue(256)]
        [SizeUnit(SizeUnit.Megabytes)]
        [ConfigurationEntry("Logs.Microsoft.RetentionSizeInMb", ConfigurationEntryScope.ServerWideOnly)]
        public Size? MicrosoftLogsRetentionSize { get; set; }

        [Description("Will determine whether to compress the Microsoft log files")]
        [DefaultValue(null)]
        [ConfigurationEntry("Logs.Microsoft.Compress", ConfigurationEntryScope.ServerWideOnly)]
        public bool? MicrosoftLogsCompress { get; set; }
        #endregion
        
    }
}
