using System.ComponentModel;
using Raven.Server.Config.Attributes;

namespace Raven.Server.Config.Categories
{
    public class UpdatesConfiguration : ConfigurationCategory
    {
        [Description("Indicates what release channel should be used to perform latest version checks")]
        [DefaultValue(ReleaseChannel.Patch)]
        [ConfigurationEntry("Updates.Channel", ConfigurationEntryScope.ServerWideOnly)]
        public ReleaseChannel Channel { get; set; }
    }

    public enum ReleaseChannel
    {
        Stable = 1,
        Patch = 2,
        Dev = 3
    }
}
