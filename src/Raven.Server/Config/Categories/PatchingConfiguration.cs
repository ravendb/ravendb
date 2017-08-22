using System.ComponentModel;
using Raven.Server.Config.Attributes;

namespace Raven.Server.Config.Categories
{
    public class PatchingConfiguration : ConfigurationCategory
    {
        /// <summary>
        /// The maximum number of steps iterations to give a script before timing out.
        /// Default: 1,000
        /// </summary>
        [DefaultValue(1_000)]
        [ConfigurationEntry("Patching.MaxStepsForScript")]
        public int MaxStepsForScript { get; set; }
    }
}
