using System.ComponentModel;
using Raven.Server.Config.Attributes;
using Raven.Server.Config.Settings;
using Sparrow.Logging;

namespace Raven.Server.Config.Categories
{
    public class DebugLoggingConfiguration : ConfigurationCategory
    {
        [DefaultValue("./Logs")]
        [ConfigurationEntry("Raven/DebugLog/Path")]
        public string Path { get; set; }

        [DefaultValue(LogMode.Operations)]
        [ConfigurationEntry("Raven/DebugLog/LogMode")]
        public LogMode LogMode{ get; set; }

        [DefaultValue(3)]
        [TimeUnit(TimeUnit.Days)]
        [ConfigurationEntry("Raven/DebugLog/RetentionTime")]
        public TimeSetting RetentionTime { get; set; }

    }
}