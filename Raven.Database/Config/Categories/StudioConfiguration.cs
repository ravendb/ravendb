using System.ComponentModel;
using Raven.Database.Config.Attributes;

namespace Raven.Database.Config.Categories
{
    public class StudioConfiguration : ConfigurationCategory
    {
        [DefaultValue(false)]
        [ConfigurationEntry("Raven/Studio/SkipCreatingIndexes")]
        [ConfigurationEntry("Raven/SkipCreatingStudioIndexes")]
        public bool SkipCreatingIndexes { get; set; }
    }
}