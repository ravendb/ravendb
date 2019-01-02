using System.ComponentModel;
using Raven.Server.Config.Attributes;
using Raven.Server.Config.Settings;
using Sparrow;
using Sparrow.Logging;

namespace Raven.Server.Config.Categories
{
    public class LogsConfiguration : ConfigurationCategory
    {
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
        [SizeUnit(SizeUnit.Megabytes)]
        [ConfigurationEntry("Logs.MaxFileSizeInMb", ConfigurationEntryScope.ServerWideOnly)]
        public Size MaxFileSize { get; set; }
    }
}
