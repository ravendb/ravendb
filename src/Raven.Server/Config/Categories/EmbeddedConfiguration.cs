using System.ComponentModel;
using Raven.Server.Config.Attributes;

namespace Raven.Server.Config.Categories
{
    public class EmbeddedConfiguration : ConfigurationCategory
    {
        [Description("Watch the parent process id and exit when it exited as well")]
        [DefaultValue(null)]
        [ConfigurationEntry("Embedded.ParentProcessId", ConfigurationEntryScope.ServerWideOnly)]
        [ConfigurationEntry("Testing.ParentProcessId", ConfigurationEntryScope.ServerWideOnly)]
        public int? ParentProcessId { get; set; }
    }
}
