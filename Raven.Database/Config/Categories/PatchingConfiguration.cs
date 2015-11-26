using System.ComponentModel;
using Raven.Database.Config.Attributes;

namespace Raven.Database.Config.Categories
{
    public class PatchingConfiguration : ConfigurationCategory
    {
        [DefaultValue(false)]
        [ConfigurationEntry("Raven/Patching/AllowScriptsToAdjustNumberOfSteps")]
        [ConfigurationEntry("Raven/AllowScriptsToAdjustNumberOfSteps")]
        public bool AllowScriptsToAdjustNumberOfSteps { get; set; }

        /// <summary>
        /// The maximum number of steps (instructions) to give a script before timing out.
        /// Default: 10,000
        /// </summary>
        [DefaultValue(10 * 1000)]
        [ConfigurationEntry("Raven/Patching/MaxStepsForScript")]
        [ConfigurationEntry("Raven/MaxStepsForScript")]
        public int MaxStepsForScript { get; set; }

        /// <summary>
        /// The number of additional steps to add to a given script based on the processed document's quota.
        /// Set to 0 to give use a fixed size quota. This value is multiplied with the doucment size.
        /// Default: 5
        /// </summary>
        [DefaultValue(5)]
        [ConfigurationEntry("Raven/Patching/AdditionalStepsForScriptBasedOnDocumentSize")]
        [ConfigurationEntry("Raven/AdditionalStepsForScriptBasedOnDocumentSize")]
        public int AdditionalStepsForScriptBasedOnDocumentSize { get; set; }
    }
}