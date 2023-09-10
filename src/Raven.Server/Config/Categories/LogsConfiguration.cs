using System.ComponentModel;
using Raven.Server.Config.Attributes;
using Raven.Server.Config.Settings;
using Sparrow;
using Sparrow.Logging;

namespace Raven.Server.Config.Categories
{
    [ConfigurationCategory(ConfigurationCategoryType.Logs)]
    public class LogsConfiguration : ConfigurationCategory
    {
        [Description("The path to the directory where the RavenDB server logs will be stored")]
        [DefaultValue("Logs")]
        [ConfigurationEntry("Logs.Path", ConfigurationEntryScope.ServerWideOnly)]
        public PathSetting Path { get; set; }

        [Description("The level of logs that will be written to the log files (None, Operations or Information)")]
        [DefaultValue(LogMode.Operations)]
        [ConfigurationEntry("Logs.Mode", ConfigurationEntryScope.ServerWideOnly)]
        public LogMode Mode { get; set; }

        [Description("Determine whether logs are timestamped in UTC or with server-local time")]
        [DefaultValue(true)]
        [ConfigurationEntry("Logs.UseUtcTime", ConfigurationEntryScope.ServerWideOnly)]
        public bool UseUtcTime { get; set; }

        [Description("The maximum log file size in megabytes")]
        [DefaultValue(128)]
        [MinValue(16)]
        [SizeUnit(SizeUnit.Megabytes)]
        [ConfigurationEntry("Logs.MaxFileSizeInMb", ConfigurationEntryScope.ServerWideOnly)]
        public Size MaxFileSize { get; set; }

        [Description("The number of hours logs are kept before they are deleted")]
        [DefaultValue(3 * 24)]
        [MinValue(24)]
        [TimeUnit(TimeUnit.Hours)]
        [ConfigurationEntry("Logs.RetentionTimeInHrs", ConfigurationEntryScope.ServerWideOnly)]
        public TimeSetting? RetentionTime { get; set; }

        [Description("The maximum log size after which older files will be deleted")]
        [DefaultValue(null)]
        [MinValue(256)]
        [SizeUnit(SizeUnit.Megabytes)]
        [ConfigurationEntry("Logs.RetentionSizeInMb", ConfigurationEntryScope.ServerWideOnly)]
        public Size? RetentionSize { get; set; }

        [Description("Determine whether to compress the log files")]
        [DefaultValue(false)]
        [ConfigurationEntry("Logs.Compress", ConfigurationEntryScope.ServerWideOnly)]
        public bool Compress { get; set; }

        #region Microsoft Logs
        [Description("Determine whether to disable Microsoft logs")]
        [DefaultValue(true)]
        [ConfigurationEntry("Logs.Microsoft.Disable", ConfigurationEntryScope.ServerWideOnly)]
        public bool DisableMicrosoftLogs { get; set; }
        
        [Description("The path to the JSON configuration file for Microsoft logs")]
        [ReadOnlyPath]
        [DefaultValue("settings.logs.microsoft.json")]
        [ConfigurationEntry("Logs.Microsoft.ConfigurationPath", ConfigurationEntryScope.ServerWideOnly)]
        public PathSetting MicrosoftLogsConfigurationPath { get; set; }
        #endregion
    }
}
