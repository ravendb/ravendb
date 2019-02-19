using System.ComponentModel;
using Raven.Server.Config.Attributes;

namespace Raven.Server.Config.Categories
{
    public class StudioConfiguration : ConfigurationCategory
    {
        [Description("The directory in which RavenDB will search the studio files, defaults to the base directory")]
        [DefaultValue(null)]
        [ConfigurationEntry("Studio.Path", ConfigurationEntryScope.ServerWideOnly)]
        public string Path { get; set; }
    }
}
