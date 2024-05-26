using System.ComponentModel;
using Raven.Server.Config.Attributes;

namespace Raven.Server.Config.Categories
{
    [ConfigurationCategory(ConfigurationCategoryType.Patching)]
    public sealed class PatchingConfiguration : ConfigurationCategory
    {
        /// <summary>
        /// The maximum number of steps iterations to give a script before timing out.
        /// Default: 10000
        /// </summary>
        [Description("Max number of steps in the script execution of a JavaScript patch")]
        [DefaultValue(10_000)]
        [ConfigurationEntry("Patching.MaxStepsForScript", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public int MaxStepsForScript { get; set; }

        [Description("Enables calling 'eval' with custom code and function constructors taking function code as string")]
        [DefaultValue(false)]
        [ConfigurationEntry("Patching.AllowStringCompilation", ConfigurationEntryScope.ServerWideOrPerDatabaseOrPerIndex)]
        public bool AllowStringCompilation { get; set; }

        [Description("Enables Strict Mode in JavaScript engine. Default: true")]
        [DefaultValue(true)]
        [ConfigurationEntry("Patching.StrictMode", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public bool StrictMode { get; set; }
    }
}
