using System.ComponentModel;
using Raven.Server.Config.Attributes;

namespace Raven.Server.Config.Categories
{
    public class TestingConfiguration : ConfigurationCategory
    {
        [Description("Watch the parent process id and exit when it exited as well")]
        [DefaultValue(null)]
        [ConfigurationEntry("Testing.ParentProcessId")]
        public int? ParentProcessId { get; set; }
    }
}
