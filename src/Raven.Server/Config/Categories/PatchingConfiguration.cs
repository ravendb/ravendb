using System.ComponentModel;
using Raven.Server.Config.Attributes;

namespace Raven.Server.Config.Categories
{
    [ConfigurationCategory(ConfigurationCategoryType.Patching)]
    public class PatchingConfiguration : ConfigurationCategory
    {
        /// <summary>
        /// The maximum number of steps iterations to give a script before timing out.
        // There is JavaScriptConfiguration to be used everywhere except for Indexing, where it can be overloaded
        // These options are left here for backward compatibility: nullable without default values to be used in priority order in case were set by user (the same approach as with indexing)
        /// </summary>
        [ConfigurationEntry("Patching.MaxStepsForScript", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        [DefaultValue(null)]
        public int? MaxStepsForScript { get; set; }

        [Description("Enables Strict Mode in JavaScript engine. Default: true")]
        [DefaultValue(null)]
        [ConfigurationEntry("Patching.StrictMode", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public bool? StrictMode { get; set; }
    }
}
