using System.ComponentModel;
using Raven.Server.Config.Attributes;

namespace Raven.Server.Config.Categories
{
    public class PatchingConfiguration : ConfigurationCategory
    {
        /// <summary>
        /// The maximum number of steps iterations to give a script before timing out.
        /// Default: 10000
        /// </summary>
        [DefaultValue(10_000)]
        [ConfigurationEntry("Patching.MaxStepsForScript", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public int MaxStepsForScript { get; set; }

        /// <summary>
        /// The maximum number of javascript scripts to be cached
        /// Default: 2048
        /// </summary>
        [DefaultValue(2048)]
        [ConfigurationEntry("Patching.MaxNumberOfCachedScripts", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public int MaxNumberOfCachedScripts { get; set; }

        [Description("Enables Strict Mode in JavaScript engine. Default: true")]
        [DefaultValue(true)]
        [ConfigurationEntry("Patching.StrictMode", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public bool StrictMode { get; set; }
    }
}
