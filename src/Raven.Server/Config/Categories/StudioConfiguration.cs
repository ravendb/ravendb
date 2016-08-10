using System.ComponentModel;
using Raven.Server.Config.Attributes;

namespace Raven.Server.Config.Categories
{
    public class StudioConfiguration : ConfigurationCategory
    {
        [Description("Control whatever the Studio default indexes will be created or not. These default indexes are only used by the UI, and are not required for RavenDB to operate.")]
        [DefaultValue(false)]
        [ConfigurationEntry("Raven/Studio/SkipCreatingIndexes")]
        [LegacyConfigurationEntry("Raven/SkipCreatingStudioIndexes")]
        public bool SkipCreatingIndexes { get; set; }
    }
}