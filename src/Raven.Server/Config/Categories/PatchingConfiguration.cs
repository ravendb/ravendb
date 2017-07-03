using System.ComponentModel;
using Raven.Server.Config.Attributes;

namespace Raven.Server.Config.Categories
{
    public class PatchingConfiguration : ConfigurationCategory
    {
        [DefaultValue(false)]
        [ConfigurationEntry("Patching.AllowScriptsToAdjustNumberOfSteps")]
        [LegacyConfigurationEntry("Raven/AllowScriptsToAdjustNumberOfSteps")]
        public bool AllowScriptsToAdjustNumberOfSteps { get; set; }

        /// <summary>
        /// The maximum number of steps (instructions) to give a script before timing out.
        /// Default: 10,000
        /// </summary>
        [DefaultValue(10 * 1000)]
        [ConfigurationEntry("Patching.MaxStepsForScript")]
        [LegacyConfigurationEntry("Raven/MaxStepsForScript")]
        public int MaxStepsForScript { get; set; }

        /// <summary>
        /// The number of additional steps to add to a given script based on the processed document's quota.
        /// Set to 0 to give use a fixed size quota. This value is multiplied with the document size.
        /// Default: 5
        /// </summary>
        [DefaultValue(5)]
        [ConfigurationEntry("Patching.AdditionalStepsForScriptBasedOnDocumentSize")]
        [LegacyConfigurationEntry("Raven/AdditionalStepsForScriptBasedOnDocumentSize")]
        public int AdditionalStepsForScriptBasedOnDocumentSize { get; set; }
    }
}