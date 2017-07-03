using System.ComponentModel;
using Raven.Server.Config.Attributes;
using Sparrow.Logging;

namespace Raven.Server.Config.Categories
{
    public class LogsConfiguration : ConfigurationCategory
    {
        [DefaultValue("Logs")]
        [ConfigurationEntry("Logs.Path")]
        public string Path { get; set; }

        [DefaultValue(LogMode.Operations)]
        [ConfigurationEntry("Logs.Mode")]
        public LogMode Mode { get; set; }
    }
}